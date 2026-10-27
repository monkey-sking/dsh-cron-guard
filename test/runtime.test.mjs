import { apply, parseScheduleMessage } from '../lib/index.js'

let pass = 0, fail = 0
const t = (name, fn) => {
  try { fn(); pass++; console.log('  ok  ', name) }
  catch (e) { fail++; console.log('  FAIL', name, '\n       ', e.stack?.split('\n').slice(0,3).join('\n        ')) }
}
const eq = (a,b,m) => { const A=JSON.stringify(a),B=JSON.stringify(b); if(A!==B) throw new Error(`${m||''} 期望 ${B} 实得 ${A}`) }

// ---- 造一个够真的假 agent：inbox 操作会同步触发 inserted 事件，复现真机时序 ----
function makeWorld({ status = 'running' } = {}) {
  const listeners = { inserted: [], approval: [] }
  const nextTurn = [], nextStep = []
  const logs = []
  let idCounter = 0

  const ctx = {
    logger: { info: (m) => logs.push(m) },
    effect: (fn) => { const off = fn(); return off },
    on: (name, fn, opts) => {
      if (name === 'agent/inbox/inserted') listeners.inserted.push(fn)
      else if (name === 'approval/request') listeners.approval.push(fn)
      return () => {}
    },
    set: () => {},
  }

  const agent = {
    id: 'agent-1',
    get status() { return status },
    set status(v) { status = v },
    inbox: {
      get nextTurn() { return [...nextTurn] },
      get nextStep() { return [...nextStep] },
      append(target, msg) {
        const arr = target === 'next-step' ? nextStep : nextTurn
        arr.push(msg)
        for (const l of listeners.inserted) l({ agent, message: msg })
      },
      remove(id) {
        for (const arr of [nextTurn, nextStep]) {
          const i = arr.findIndex((m) => m.id === id)
          if (i >= 0) { arr.splice(i, 1); return true }
        }
        return false
      },
      replace(id, msg) {
        for (const arr of [nextTurn, nextStep]) {
          const i = arr.findIndex((m) => m.id === id)
          if (i >= 0) { arr[i] = msg; return true }
        }
        return false
      },
    },
  }
  // 模拟 DSH 的 send()：先 splice 进 next-turn，再触发事件
  const deliver = (text, occurrenceAt, scheduleId = 'schedule-A') => {
    const msg = {
      id: `msg-${++idCounter}`,
      source: { kind: 'schedule' },
      content: [{ type: 'text', text:
`[SCHEDULE REMINDER]
This is a scheduled message from the user
schedule_id_json: "${scheduleId}"
occurrence_at: ${occurrenceAt}
reminder_prompt_json: "做点事"` }],
    }
    nextTurn.push(msg)
    for (const l of listeners.inserted) l({ agent, message: msg })
    return msg
  }
  const deliverUser = (text) => {
    const msg = { id:`u-${++idCounter}`, source:{kind:'user'}, content:[{type:'text',text}] }
    nextTurn.push(msg)
    for (const l of listeners.inserted) l({ agent, message: msg })
    return msg
  }
  return { ctx, agent, nextTurn, nextStep, logs, deliver, deliverUser, listeners }
}

const fresh = () => new Date(Date.now() - 1000).toISOString()   // 1 秒前，新鲜
const stale = () => new Date(Date.now() - 3600_000).toISOString() // 1 小时前，过期

t('会话忙：schedule 消息从 next-turn 搬到 next-step', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 0 })
  w.deliver('x', fresh())
  eq(w.nextTurn.length, 0, 'next-turn 应已空')
  eq(w.nextStep.length, 1, 'next-step 应有 1 条')
})

t('会话空闲：保持原生行为不动', () => {
  const w = makeWorld({ status: 'idle' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 0 })
  w.deliver('x', fresh())
  eq(w.nextTurn.length, 1)
  eq(w.nextStep.length, 0)
})

t('用户消息永远不搬', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 0 })
  w.deliverUser('我现在很急')
  eq(w.nextTurn.length, 1)
  eq(w.nextStep.length, 0)
})

t('过期唤醒被丢弃', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 600000, dedupeWindowMs: 0 })
  w.deliver('x', stale())
  eq(w.nextTurn.length + w.nextStep.length, 0, '应被丢掉')
  if (!w.logs.some(l => l.includes('丢弃过期'))) throw new Error('没有记日志')
})

t('新鲜唤醒不被丢', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 600000, dedupeWindowMs: 0 })
  w.deliver('x', fresh())
  eq(w.nextStep.length, 1)
})

t('防递归：搬移触发的二次事件不再无限套娃', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 0 })
  // append('next-step') 会再触发一次 inserted；若护栏失效这里会栈溢出
  w.deliver('x', fresh())
  eq(w.nextStep.length, 1, '只应有一条，不能重复')
})

t('同源合并：同 scheduleId 更旧的被撤掉', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 600000 })
  const older = new Date(Date.now() - 120_000).toISOString()
  // 第一条：让它在 next-step 落定（忙 → 搬走）
  w.deliver('a', older, 'schedule-A')
  // 第二条：更晚的 occurrence
  w.deliver('b', fresh(), 'schedule-A')
  eq(w.nextTurn.length + w.nextStep.length, 1, '合并后只剩 1 条')
})

t('同源合并：不同 scheduleId 各自保留', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 600000 })
  w.deliver('a', fresh(), 'schedule-A')
  w.deliver('b', fresh(), 'schedule-B')
  eq(w.nextTurn.length + w.nextStep.length, 2)
})

t('dryRun：只记日志不动队列', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 600000, dedupeWindowMs: 0, dryRun: true })
  w.deliver('x', stale())
  eq(w.nextTurn.length, 1, 'dry-run 不能真的删')
  if (!w.logs.some(l => l.includes('dry-run'))) throw new Error('没有 dry-run 日志')
})

t('enabled:false 完全不介入', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { enabled: false })
  w.deliver('x', stale())
  eq(w.nextTurn.length, 1)
  eq(w.nextStep.length, 0)
})

t('relocateToNextStep:false 时仍做过期治理', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { relocateToNextStep: false, staleSkipMs: 600000, dedupeWindowMs: 0 })
  w.deliver('x', stale())
  eq(w.nextTurn.length + w.nextStep.length, 0, '过期仍应丢弃')
})

t('解析不了的消息原样放行（绝不误丢）', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 1, dedupeWindowMs: 0 })
  const msg = { id:'weird', source:{kind:'schedule'}, content:[{type:'text',text:'[SCHEDULE REMINDER]\n但格式是坏的'}] }
  w.nextTurn.push(msg)
  for (const l of w.listeners.inserted) l({ agent: w.agent, message: msg })
  const alive = [...w.nextTurn, ...w.nextStep].some(m => m.id === 'weird')
  if (!alive) throw new Error('不该丢：解析失败必须原样放行')
})

t('让路用户：next-turn 有用户消息时不插队', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 0, yieldToUser: true })
  // 用户先排队
  const u = w.deliverUser('我这个很急')
  // 再来一条定时唤醒
  w.deliver('x', fresh())
  // 唤醒必须留在 next-turn（排在用户后面），不能跑到 next-step 插队
  eq(w.nextStep.length, 0, '不应插到用户前面')
  if (!w.nextTurn.some(m => m.source.kind === 'schedule')) throw new Error('唤醒应仍在 next-turn')
  if (!w.logs.some(l => l.includes('让路'))) throw new Error('应记录让路日志')
})

t('yieldToUser:false 时照常插队（可配置回退）', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 0, dedupeWindowMs: 0, yieldToUser: false })
  w.deliverUser('我这个很急')
  w.deliver('x', fresh())
  eq(w.nextStep.length, 1, '关掉让路就恢复插队')
})

t('回归：裁剪后再搬移，搬的必须是裁剪后的内容', () => {
  const w = makeWorld({ status: 'running' })
  apply(w.ctx, { staleSkipMs: 900000, dedupeWindowMs: 0 })
  // 造一条批量消息：一条过期、一条新鲜
  const expired = new Date(Date.now() - 3600_000).toISOString()
  const ok = new Date(Date.now() - 60_000).toISOString()
  const text = `[SCHEDULE REMINDER BATCH]
This is a scheduled message from the user
reminders_json: [{"schedule_id":"s-old","occurrence_at":"${expired}","reminder_prompt":"旧的"},{"schedule_id":"s-new","occurrence_at":"${ok}","reminder_prompt":"新的"}]`
  const msg = { id:'batch1', source:{kind:'schedule'}, content:[{type:'text',text}] }
  w.nextTurn.push(msg)
  for (const l of w.listeners.inserted) l({ agent: w.agent, message: msg })

  eq(w.nextTurn.length, 0, '应已搬走')
  eq(w.nextStep.length, 1, '应落在 next-step')
  const delivered = w.nextStep[0]
  const parsed = parseScheduleMessage(delivered.content.filter(p=>p.type==='text').map(p=>p.text).join(''))
  eq(parsed.items.length, 1, '只应剩 1 条')
  eq(parsed.items[0].scheduleId, 's-new', '留下的必须是新鲜那条')
  if (parsed.items[0].prompt !== '新的') throw new Error('内容串了: '+parsed.items[0].prompt)
})

console.log(`\n${pass} 通过, ${fail} 失败`)
process.exit(fail ? 1 : 0)
