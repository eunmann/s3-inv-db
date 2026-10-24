export const meta = {
  name: 'review-round',
  description: 'One review-loop round: finders per path group, dedup, one fixer in the main checkout',
  phases: [{ title: 'Find' }, { title: 'Fix' }],
}
const A = args
const FINDINGS = {
  type: 'object',
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          id: { type: 'string' }, kind: { type: 'string' }, severity: { type: 'string' },
          file: { type: 'string' }, line: { type: 'integer' }, summary: { type: 'string' },
          evidence: { type: 'string' }, failure_scenario: { type: 'string' }, suggested_fix: { type: 'string' },
        },
        required: ['id', 'kind', 'severity', 'file', 'line', 'summary', 'evidence', 'failure_scenario'],
      },
    },
  },
  required: ['findings'],
}
const checkout = A.checkout || 'the main checkout'
// The custom agents load when a session starts; in a session that added or
// changed them, pass inlineAgents: true to run general-purpose agents that
// read the same definitions from the checkout.
const role = name => A.inlineAgents
  ? { agentType: 'general-purpose', preamble: `Your role, rules and output format are defined in ${checkout}/.claude/agents/${name}.md. Read that file first (ignore its YAML frontmatter) and follow it exactly.\n\n` }
  : { agentType: name, preamble: '' }
const finder = role('review-finder')
const fixer = role('review-fixer')

phase('Find')
const found = await parallel(A.groups.map(g => () =>
  agent(
    `${finder.preamble}Mode: ${A.mode}. Range: ${A.range}. Checkout: ${checkout} (run git and read files there). Group ${g.name}, paths: ${g.paths.join(' ')}.`,
    { agentType: finder.agentType, label: `R${A.round} · ${g.name} · find`, phase: 'Find', schema: FINDINGS })
    .then(r => ({ g, findings: r ? r.findings : null }), () => ({ g, findings: null }))))
// A finder that failed reviewed nothing; report it so the coordinator never
// mistakes a failed round for a clean one.
const failed = found.filter(r => r.findings === null).map(r => r.g.name)
if (failed.length) log(`finders failed: ${failed.join(', ')}`)
const findings = []
for (const r of found.filter(r => r.findings)) {
  for (const f of r.findings) {
    if (!findings.some(s => s.file === f.file && Math.abs(s.line - f.line) <= 3)) findings.push(f)
  }
}
const order = { high: 0, medium: 1, low: 2 }
findings.sort((a, b) => (order[a.severity] ?? 3) - (order[b.severity] ?? 3))
log(`findings after dedup: ${findings.length}`)
let reply = null
if (findings.length) {
  phase('Fix')
  reply = await agent(
    `${fixer.preamble}Round ${A.round}. Checkout: ${checkout}. Findings:\n${JSON.stringify(findings, null, 1)}\n\nCommit trailer:\n${A.trailer}`,
    { agentType: fixer.agentType, label: `R${A.round} · fix`, phase: 'Fix' })
}
return {
  failedFinders: failed,
  findings: findings.map(f => `${f.id} [${f.severity}/${f.kind}] ${f.file}:${f.line} ${f.summary}`),
  fixer: reply,
}
