import { createRequire } from 'node:module'
const require = createRequire(import.meta.url)

// 用真实 cordis 运行时 + 真实 dsh-agent-loop 的 ReactLoopInbox 语义，
// 验证插件能被宿主 plugin() 装载、能实际改写队列。
const { Context } = require('/opt/homebrew/lib/node_modules/@deepseek-ai/dsh/node_modules/@deepseek-ai/cordis')
  ?? {}

let pass = 0
let fail = 0
function check(name, cond, extra) {
  if (cond) {
    pass += 1
    console.log(`  ✓ ${name}`)
  } else {
    fail += 1
    console.log(`  ✗ ${name}${extra ? `  ← ${extra}` : ''}`)
  }
}

// ── 1. 宿主能否 plugin() 装载本插件 ──────────────────────────────
console.log('1. 宿主装载')
const mod = await import('../lib/index.js')
const { apply } = mod
const ctx = new Context()
const app = ctx.plugin({
  name: 'dsh-cron-guard',
  apply,
})

check('导出 name', mod.name === 'dsh-cron-guard')
check('导出 apply 函数', typeof mod.apply === 'function')
check('导出 inject 数组', Array.isArray(mod.inject))
check('插件被 cordis 装载后 fiber 存在', !!app)

// ── 2. 真实事件时序：inserted 同步触发 ───────────────────────────
console.log('2. 事件时序')
const order = []
const ctx2 = new Context()
ctx2.on('agent/inbox/inserted', () => order.push('inserted'))
ctx2.on('agent/wake', () => order.push('wake'))
ctx2.emit('agent/inbox/inserted', {})
ctx2.emit('agent/wake', {})
check('inserted 先于 wake', order[0] === 'inserted' && order[1] === 'wake', order.join(','))

// ── 3. 服务缺失时降级而不是崩溃 ─────────────────────────────────
console.log('3. 降级')
const ctx3 = new Context()
let threw = null
try {
  ctx3.plugin({ name: 'probe', apply: (c) => mod.apply(c, { enabled: true }) })
  await new Promise((r) => setTimeout(r, 100))
} catch (e) {
  threw = e
}
check('无 agents/approval 服务时不抛错', threw === null, threw?.message)

// ── 4. 真机时序回归：搬移后消息仍在队列且落到 next-step ─────────
console.log('4. 队列改写')
const inboxEvents = []
const fakeAgent = {
  status: 'running',
  session: { id: 's1' },
  inbox: {
    nextTurn: [],
    nextStep: [],
    // 忠实复刻 real emitter：每条消息单独 emit 一次，载荷带 agent
    append(t, m) {
      ;(t === 'next-turn' ? this.nextTurn : this.nextStep).push(m)
      ctx4.emit('agent/inbox/inserted', { message: m, agent: fakeAgent })
    },
    // 忠实复刻 real Inbox.remove：真的把消息摘掉
    remove(id) {
      for (const t of ['nextTurn', 'nextStep']) {
        const i = this[t].findIndex((m) => m.id === id)
        if (i >= 0) { this[t].splice(i, 1); return true }
      }
      return false
    },
    replace(id, m) {
      for (const t of ['nextTurn', 'nextStep']) {
        const i = this[t].findIndex((x) => x.id === id)
        if (i >= 0) { this[t][i] = m; return true }
      }
      return false
    },
    splice(t, start, del, ins) {
      this.append(t, ins[0])
    },
  },
}
const ctx4 = new Context()
ctx4.agents = { list: () => [fakeAgent], get: () => fakeAgent }
ctx4.plugin({
  name: 'cron-guard',
  apply: (c) => apply(c, { enabled: true, relocateToNextStep: true, yieldToUser: true, staleSkipMs: 0, dedupeWindowMs: 0, approvalTimeoutMs: 0 }),
})
await new Promise((r) => setTimeout(r, 100))

const msg = {
  id: 'm1',
  source: { kind: 'schedule', scheduleId: 'sch-1', occurrenceAt: new Date().toISOString() },
  content: [{ type: 'text', text: '[SCHEDULE REMINDER]\nschedule_id_json: "sch-1"\noccurrence_at: 2026-10-24T03:00:00.000Z\nreminder_prompt_json: "检查 X 回复"' }],
}
fakeAgent.inbox.append('next-turn', msg)
await new Promise((r) => setTimeout(r, 50))

check('消息仍在队列（没丢）', fakeAgent.inbox.nextTurn.length + fakeAgent.inbox.nextStep.length === 1)
check('已搬到 next-step', fakeAgent.inbox.nextStep.length === 1, `nextTurn=${fakeAgent.inbox.nextTurn.length} nextStep=${fakeAgent.inbox.nextStep.length}`)
check('搬移后仍是同一消息', fakeAgent.inbox.nextStep[0]?.id === 'm1')

// ── 5. 追加失败时必须回退，不能丢消息 ───────────────────────────
console.log('5. 失败回退')
const ctx5 = new Context()
const lost = { id: 'm9', source: { kind: 'schedule', scheduleId: 's9', occurrenceAt: new Date().toISOString() }, content: [{ type: 'text', text: '[SCHEDULE REMINDER]\nschedule_id_json: "s9"\noccurrence_at: 2026-10-24T03:00:00.000Z\nreminder_prompt_json: "x"' }] }
const flaky = {
  status: 'running',
  inbox: {
    nextTurn: [lost],
    nextStep: [],
    remove(id) { const i = this.nextTurn.findIndex((m) => m.id === id); if (i >= 0) { this.nextTurn.splice(i, 1); return true } return false },
    // 往 next-step 追加时故意炸掉，模拟投影写入失败
    append(t, m) {
      if (t === 'next-step') throw new Error('projection write failed')
      this.nextTurn.push(m)
    },
  },
}
ctx5.plugin({ name: 'g5', apply: (c) => apply(c, { enabled: true, relocateToNextStep: true, yieldToUser: false, staleSkipMs: 0, dedupeWindowMs: 0, approvalTimeoutMs: 0 }) })
await new Promise((r) => setTimeout(r, 50))
ctx5.emit('agent/inbox/inserted', { message: lost, agent: flaky })
await new Promise((r) => setTimeout(r, 50))
const total = flaky.inbox.nextTurn.length + flaky.inbox.nextStep.length
check('追加失败后消息仍在（没丢）', total === 1, `nextTurn=${flaky.inbox.nextTurn.length} nextStep=${flaky.inbox.nextStep.length}`)
check('回退到了 next-turn', flaky.inbox.nextTurn.length === 1)

console.log(`\n${pass} 通过, ${fail} 失败`)
process.exit(fail === 0 ? 0 : 1)
