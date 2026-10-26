// Live job updates. One EventSource per page (on pages that carry
// [data-live-jobs]) receives job snapshots from /api/jobs/stream and:
//
//   - updates progress bars and progress text in place, so a running
//     build costs no HTML requests and its bar animates instead of
//     being replaced;
//   - asks an element for fresh HTML (htmx event "live-refresh") only
//     when what it shows changed shape: a job started, changed state,
//     or (for elements with data-live-stage) changed stage; or the
//     catalog changed the inventory outside a job;
//   - after a reconnect, refreshes elements that still show a live job
//     the server no longer has (it finished while we were away);
//   - polls live elements while the stream is down, and after a few
//     seconds down shows [data-live-status] (layout.html);
//   - tells screen readers, through [data-live-announce] (layout.html),
//     when a build it watched ends.
//
// A page marked data-live-jobs="while-visible" closes its stream while
// the tab is hidden, so background tabs don't hold one of the
// browser's few connections to the origin; on return it reconnects and
// re-renders its [data-live-any] blocks for whatever it missed. Only a
// page whose live content is all [data-live-any] can do this: a row
// would miss "inventory" frames.
//
// Elements opt in with data-live-inv="<inventory id>" plus the state
// they were rendered with: data-job-id, data-job-state and, when they
// care about stages, data-live-stage. Inside them, [data-live-bar] and
// [data-live-text] are kept current. [data-live-any] elements refresh
// on any job state change. [data-elapsed-since] (RFC 3339) renders a
// ticking "elapsed" clock and [data-deadline] a "~T left" countdown.
(function () {
    'use strict';
    if (!document.querySelector('[data-live-jobs]') || !window.EventSource || !window.htmx) return;

    var BUILD_STAGES = ['downloading', 'building', 'finalizing'];
    var FETCH_STAGES = ['fetching'];
    var LIVE = { queued: true, running: true };

    var latest = {};     // inventory id -> newest job snapshot
    var receivedAt = {}; // inventory id -> when it arrived
    var requestedAt = {}; // live element id -> when its HTML was last requested
    var eta = {};        // job id + counter -> smoothed ETA state
    var snapshotIDs = null;
    var connected = false;
    var source = null;
    var retryMs = 2000;
    var retryTimer = null;
    var statusTimer = null;
    var whileVisible = !!document.querySelector('[data-live-jobs="while-visible"]');
    var paused = false;  // closed while the tab is hidden
    var resumed = false; // reconnected after a pause; next sync re-renders

    function liveElements(inv) {
        var out = [];
        document.querySelectorAll('[data-live-inv]').forEach(function (el) {
            if (el.getAttribute('data-live-inv') === inv) out.push(el);
        });
        return out;
    }

    // refresh asks el for fresh HTML, coalescing bursts into one request.
    function refresh(el, delay) {
        if (el._liveTimer) return;
        el._liveTimer = setTimeout(function () {
            el._liveTimer = null;
            if (document.body.contains(el)) window.htmx.trigger(el, 'live-refresh');
        }, delay || 80);
    }

    // stale reports whether el's rendered state no longer matches job.
    function stale(el, job) {
        if (!job) return false;
        var id = el.getAttribute('data-job-id') || '';
        var state = el.getAttribute('data-job-state') || '';
        if (id !== job.ID) return true;
        if (state !== job.State) return true;
        if (el.hasAttribute('data-live-stage') && el.getAttribute('data-live-stage') !== (job.Stage || '')) return true;
        return false;
    }

    function pct(job) {
        if (!job.StageTotal || job.StageTotal <= 0 || !job.StageDone || job.StageDone <= 0) return 0;
        if (job.StageDone >= job.StageTotal) return 100;
        return Math.floor(job.StageDone * 100 / job.StageTotal);
    }

    // counterStart is when the current progress counter started: the
    // in-progress stage's start time.
    function counterStart(job) {
        if (job.StepStartedAt) return Date.parse(job.StepStartedAt);
        var stages = job.Stages || [];
        for (var i = stages.length - 1; i >= 0; i--) {
            if (stages[i].n === job.Stage && stages[i].s) return Date.parse(stages[i].s);
        }
        return NaN;
    }

    // remaining estimates the current counter's time left in ms, smoothed
    // so it counts down between updates instead of jumping with each.
    function remaining(job, now) {
        var key = job.ID + '|' + job.Stage + '|' + (job.Step || '');
        var done = job.StageDone, total = job.StageTotal;
        var start = counterStart(job);
        var elapsed = now - start;
        if (!(done > 0) || !(total > done) || isNaN(start) || elapsed < 2000 || done / total < 0.02) {
            delete eta[key];
            return NaN;
        }
        var fresh = elapsed * (total - done) / done;
        var st = eta[key];
        if (!st) {
            st = eta[key] = { deadline: now + fresh, done: done };
        } else if (st.done !== done) {
            // New progress: move the deadline part of the way toward the
            // new estimate, so one fast or slow tick doesn't swing it.
            st.deadline = 0.6 * st.deadline + 0.4 * (now + fresh);
            st.done = done;
        }
        return Math.max(0, st.deadline - now);
    }

    // formatDuration matches the server's clock helper (templates
    // clock): whole seconds, "1m5s", "2h3m".
    function formatDuration(ms) {
        var s = Math.floor(ms / 1000);
        if (s < 60) return s + 's';
        var m = Math.floor(s / 60);
        if (m < 60) return m + 'm' + (s % 60 ? (s % 60) + 's' : '');
        var h = Math.floor(m / 60);
        return h + 'h' + (m % 60 ? (m % 60) + 'm' : '');
    }

    function formatETA(ms) {
        if (isNaN(ms)) return '';
        if (ms < 5000) return 'almost done';
        // Coarser steps the further out it is: a long estimate is rough.
        var s = ms / 1000;
        if (s < 60) return '~' + Math.ceil(s / 5) * 5 + 's left';
        if (s < 600) return '~' + formatDuration(Math.ceil(s / 10) * 10000) + ' left';
        return '~' + formatDuration(Math.ceil(s / 60) * 60000) + ' left';
    }

    function progressText(job, now) {
        if (job.State === 'queued') return 'Queued — waiting for a free build slot';
        var parts = [job.StageLabel || job.Stage || 'Starting…'];
        if (job.Step) parts[0] += ' · ' + job.Step;
        var p = pct(job);
        if (p > 0) parts.push(p + '%');
        var e = formatETA(remaining(job, now));
        if (e) parts.push(e);
        return parts.join(' · ');
    }

    // segments returns one {fill, active} per pipeline stage: stages
    // before the current one full, the current one at its counter (or
    // active with no fill while it has no total), later ones empty. The
    // bar as a whole never moves backwards when a stage starts.
    // Mirrors liveSegments in internal/templates/live.go. Merging spilled
    // runs happens inside the build segment, before the index build's
    // own counter starts, so it shows there as indeterminate.
    function segments(job) {
        var order = job.Stage === 'fetching' ? FETCH_STAGES : BUILD_STAGES;
        var idx = order.indexOf(job.Stage);
        var counted = true;
        if (job.Stage === 'merging') { idx = 1; counted = false; }
        var fill = counted ? stageFill(job) : 0;
        return order.map(function (name, i) {
            if (job.State === 'succeeded') return { fill: 100, active: false };
            if (idx < 0) return { fill: 0, active: i === 0 && job.State === 'running' };
            if (i < idx) return { fill: 100, active: false };
            if (i > idx) return { fill: 0, active: false };
            return { fill: fill, active: LIVE[job.State] === true };
        });
    }

    // stageFill is how full the current stage's segment is. A stage with
    // steps (finalizing) restarts its counter at every step, and how
    // many steps are left is unknown, so each step fills half of what
    // the stage has left: the segment only ever grows, reaches the end
    // only when the stage does, and a new step never empties it.
    var stepFill = {}; // job id -> {stage, step, base}
    function stageFill(job) {
        if (!job.Step) return pct(job);
        var st = stepFill[job.ID];
        if (!st || st.stage !== job.Stage) {
            st = stepFill[job.ID] = { stage: job.Stage, step: job.Step, base: 0 };
        } else if (st.step !== job.Step) {
            st.base += (1 - st.base) / 2;
            st.step = job.Step;
        }
        return Math.floor(100 * (st.base + (1 - st.base) / 2 * pct(job) / 100));
    }

    function renderBar(bar, job) {
        var segs = segments(job);
        var cells = bar.querySelectorAll('[data-seg]');
        if (cells.length !== segs.length) {
            bar.innerHTML = '';
            segs.forEach(function () {
                var seg = document.createElement('div');
                seg.setAttribute('data-seg', '');
                seg.className = 'live-seg';
                seg.appendChild(document.createElement('div'));
                bar.appendChild(seg);
            });
            cells = bar.querySelectorAll('[data-seg]');
        }
        var total = 0;
        segs.forEach(function (s, i) {
            var cell = cells[i];
            cell.classList.toggle('is-indeterminate', s.active && s.fill === 0);
            cell.firstChild.style.width = s.fill + '%';
            total += s.fill;
        });
        bar.setAttribute('aria-valuenow', String(Math.round(total / segs.length)));
    }

    function apply(el, job, now) {
        if (!job || job.ID !== el.getAttribute('data-job-id')) return;
        el.querySelectorAll('[data-live-bar]').forEach(function (bar) { renderBar(bar, job); });
        el.querySelectorAll('[data-live-text]').forEach(function (t) {
            var text = progressText(job, now);
            if (t.textContent !== text) t.textContent = text;
        });
    }

    function onJob(job) {
        var inv = job.InventoryID;
        var prev = latest[inv];
        latest[inv] = job;
        receivedAt[inv] = Date.now();
        if (snapshotIDs) snapshotIDs[inv] = true;
        var now = Date.now();
        liveElements(inv).forEach(function (el) {
            if (stale(el, job)) refresh(el);
            else apply(el, job, now);
        });
        if (!prev || prev.ID !== job.ID || prev.State !== job.State) refreshAny();
        announceEnd(job);
    }

    // The connect snapshot carries only queued and running jobs, so a
    // build seen in an end state ended while this page watched it.
    var ENDS = { succeeded: 'Build finished', failed: 'Build failed', cancelled: 'Build cancelled' };
    var announced = {}; // job id -> its end was announced
    var pendingNews = [];
    function announceEnd(job) {
        var what = job.Kind === 'build' && ENDS[job.State];
        if (!what || announced[job.ID]) return;
        announced[job.ID] = true;
        pendingNews.push(what + ': ' + runName(job.InventoryID));
        if (pendingNews.length > 1) return;
        // Builds ending together are read as one message.
        setTimeout(function () {
            var region = document.querySelector('[data-live-announce]');
            if (region) region.textContent = pendingNews.join('. ');
            pendingNews = [];
        }, 100);
    }

    // runName is "<source>/<inventory> <run>", the run as the pages
    // show it (humanfmt.RunTimestamp): 2026-01-02T03-04Z reads
    // "2026-01-02 03:04 UTC".
    function runName(inv) {
        var parts = String(inv).split('/');
        if (parts.length !== 3) return String(inv);
        var m = /^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})(?:-\d{2})?Z$/.exec(parts[2]);
        return parts[0] + '/' + parts[1] + ' ' + (m ? m[1] + ' ' + m[2] + ':' + m[3] + ' UTC' : parts[2]);
    }

    function refreshAny() {
        document.querySelectorAll('[data-live-any]').forEach(function (el) { refresh(el, 400); });
    }

    function onInventory(inv) {
        liveElements(inv).forEach(function (el) { refresh(el); });
        refreshAny();
    }

    // onSynced runs once the connect snapshot is complete: any element
    // still showing a live job the server no longer reports finished
    // while we were disconnected.
    function onSynced() {
        var live = snapshotIDs || {};
        snapshotIDs = null;
        document.querySelectorAll('[data-live-inv]').forEach(function (el) {
            if (LIVE[el.getAttribute('data-job-state')] && !live[el.getAttribute('data-live-inv')]) refresh(el);
        });
        if (resumed) {
            resumed = false;
            refreshAny();
        }
    }

    // setConnected records the stream's state; the notice appears only
    // once it has been down a few seconds, so a reconnect blip doesn't
    // flash it, and never while the stream is closed on purpose. The
    // timer is armed once per outage: the browser retries a dropped
    // stream every couple of seconds, each retry failing with another
    // error, and re-arming on each would keep the notice from showing.
    function setConnected(on) {
        connected = on;
        if (on || paused) {
            clearTimeout(statusTimer);
            statusTimer = null;
            showStatus(false);
        } else if (statusTimer === null) {
            statusTimer = setTimeout(function () { showStatus(true); }, 3000);
        }
    }

    function showStatus(down) {
        document.querySelectorAll('[data-live-status]').forEach(function (el) {
            el.hidden = !down;
        });
    }

    function connect() {
        retryTimer = null;
        if (paused) return;
        var es = source = new EventSource('/api/jobs/stream');
        es.addEventListener('open', function () {
            retryMs = 2000;
            snapshotIDs = {};
            setConnected(true);
        });
        es.addEventListener('job', function (e) {
            try { onJob(JSON.parse(e.data)); } catch (_) { /* malformed frame: skip */ }
        });
        es.addEventListener('inventory', function (e) {
            try { onInventory(JSON.parse(e.data).InventoryID); } catch (_) { /* skip */ }
        });
        es.addEventListener('synced', onSynced);
        es.addEventListener('error', function () {
            if (source !== es) return;
            setConnected(false);
            // The browser retries a dropped stream by itself; a refused
            // one (429, 5xx) is closed for good, so retry it here.
            if (es.readyState === EventSource.CLOSED) {
                source = null;
                retryTimer = setTimeout(connect, retryMs);
                retryMs = Math.min(retryMs * 2, 60000);
            }
        });
    }

    function pause() {
        paused = true;
        clearTimeout(retryTimer);
        retryTimer = null;
        if (source) source.close();
        source = null;
        setConnected(false);
    }

    function resume() {
        paused = false;
        resumed = true;
        retryMs = 2000;
        connect();
    }

    // Remember when each live element's HTML was asked for, by the
    // element the response replaces (its id survives the swap), so a
    // response that crossed a newer event in flight can be corrected.
    // Keyed by element, not inventory: the drawer asking for its own
    // HTML says nothing about how fresh the row's in-flight response is.
    // A request that swaps nothing (pin, cancel) replaces no HTML.
    document.body.addEventListener('htmx:beforeRequest', function (e) {
        var elt = e.detail && e.detail.elt;
        if (elt && elt.getAttribute && elt.getAttribute('hx-swap') === 'none') return;
        var t = e.detail && e.detail.target;
        var el = t && t.closest && t.closest('[data-live-inv]');
        if (el && el.id) requestedAt[el.id] = Date.now();
    });

    // New or swapped-in content: bring it up to date with what the
    // stream has said since its HTML was requested. afterSwap runs in
    // the same task as the swap, before the browser paints, so the
    // server-rendered progress line and clocks never flash.
    function reconcile() {
        var now = Date.now();
        document.querySelectorAll('[data-live-inv]').forEach(function (el) {
            var inv = el.getAttribute('data-live-inv');
            var job = latest[inv];
            if (!job) return;
            if (stale(el, job) && (receivedAt[inv] || 0) >= (requestedAt[el.id] || 0)) refresh(el);
            else apply(el, job, now);
        });
        tickElapsed(document, now);
    }
    document.body.addEventListener('htmx:afterSwap', reconcile);
    // Back and Forward refetch the page and swap it in without an
    // afterSwap.
    document.body.addEventListener('htmx:historyRestore', reconcile);

    function tickElapsed(root, now) {
        (root || document).querySelectorAll('[data-elapsed-since]').forEach(function (el) {
            var t = Date.parse(el.getAttribute('data-elapsed-since'));
            if (isNaN(t)) return;
            var text = formatDuration(Math.max(0, now - t));
            if (el.textContent !== text) el.textContent = text;
        });
        (root || document).querySelectorAll('[data-deadline]').forEach(function (el) {
            var t = Date.parse(el.getAttribute('data-deadline'));
            if (isNaN(t)) return;
            var text = formatETA(Math.max(0, t - now)) || '';
            if (el.textContent !== text) el.textContent = text;
        });
    }

    // Once a second: tick clocks, count ETAs down, and while the stream
    // is down, poll anything still showing a live job.
    var tick = 0;
    setInterval(function () {
        var now = Date.now();
        tickElapsed(document, now);
        document.querySelectorAll('[data-live-inv]').forEach(function (el) {
            var job = latest[el.getAttribute('data-live-inv')];
            if (job && LIVE[job.State]) {
                el.querySelectorAll('[data-live-text]').forEach(function (t) {
                    var text = progressText(job, now);
                    if (t.textContent !== text) t.textContent = text;
                });
            }
        });
        tick++;
        if (!connected && !paused && tick % 5 === 0) {
            document.querySelectorAll('[data-live-inv]').forEach(function (el) {
                if (LIVE[el.getAttribute('data-job-state')]) refresh(el);
            });
        }
    }, 1000);

    if (whileVisible) {
        document.addEventListener('visibilitychange', function () {
            if (document.hidden) pause();
            else if (paused) resume();
        });
    }
    reconcile();
    if (whileVisible && document.hidden) pause();
    else connect();
})();
