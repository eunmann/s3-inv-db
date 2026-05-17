// Active TOC link via IntersectionObserver — highlights whichever
// section is currently in view as you scroll.
(function () {
    var toc = document.querySelector('.helpdoc .toc');
    if (!toc) return;
    var sections = document.querySelectorAll('.helpdoc section[id], .helpdoc h2[id], .helpdoc h3[id]');
    if (!sections.length) return;
    var linkFor = {};
    toc.querySelectorAll('a[href^="#"]').forEach(function (a) {
        linkFor[a.getAttribute('href').slice(1)] = a;
    });
    var active = null;
    function setActive(id) {
        if (active === id) return;
        if (active && linkFor[active]) linkFor[active].classList.remove('active');
        active = id;
        if (linkFor[id]) linkFor[id].classList.add('active');
    }
    var obs = new IntersectionObserver(function (entries) {
        var visible = entries.filter(function (e) { return e.isIntersecting; });
        if (!visible.length) return;
        visible.sort(function (a, b) { return a.target.getBoundingClientRect().top - b.target.getBoundingClientRect().top; });
        setActive(visible[0].target.id);
    }, { rootMargin: '-80px 0px -70% 0px', threshold: 0 });
    sections.forEach(function (s) { obs.observe(s); });
})();

// Troubleshooting filter.
(function () {
    var input = document.getElementById('ts-filter');
    if (!input) return;
    var rows = document.querySelectorAll('[data-search-target]');
    var empty = document.getElementById('ts-empty');
    input.addEventListener('input', function () {
        var q = input.value.trim().toLowerCase();
        var anyVisible = false;
        rows.forEach(function (row) {
            var hay = (row.getAttribute('data-search-target') + ' ' + row.textContent).toLowerCase();
            var match = !q || hay.indexOf(q) !== -1;
            row.classList.toggle('hidden-by-search', !match);
            if (match) anyVisible = true;
        });
        empty.classList.toggle('hidden', anyVisible);
    });
})();
