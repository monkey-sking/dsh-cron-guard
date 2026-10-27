import { parseScheduleMessage, decideDisposition, classifyStaleness, rebuildTrimmedMessage, findSuperseded, messageText, sourceKindOf } from '../lib/index.js'

let pass = 0, fail = 0
const t = (name, fn) => {
  try { fn(); pass++; console.log('  ok  ', name) }
  catch (e) { fail++; console.log('  FAIL', name, '\n       ', e.message) }
}
const eq = (a, b, m) => { const A=JSON.stringify(a), B=JSON.stringify(b); if (A!==B) throw new Error(`${m||''} 期望 ${B} 实得 ${A}`) }

// 真实单条框架（取自会话日志）
const SINGLE = `[SCHEDULE REMINDER]
This is a scheduled message from the user
schedule_id_json: "schedule-52512284-a74d-44b1-9f74-b2a347c87003"
occurrence_at: 2026-10-22T04:30:21.585Z
reminder_prompt_json: "X 回复探针发现新回复。\\n\\n按规则处理。"`

t('解析单条框架', () => {
  const p = parseScheduleMessage(SINGLE)
  if (!p) throw new Error('解析失败')
  eq(p.batch, false)
  eq(p.items.length, 1)
  eq(p.items[0].scheduleId, 'schedule-52512284-a74d-44b1-9f74-b2a347c87003')
  eq(p.items[0].occurrenceAt, '2026-10-22T04:30:21.585Z')
  if (!p.items[0].prompt.includes('X 回复探针')) throw new Error('prompt 没解出来: '+p.items[0].prompt)
})

t('非 schedule 消息返回 null', () => {
  eq(parseScheduleMessage('随便一句话'), null)
  eq(parseScheduleMessage(''), null)
  eq(parseScheduleMessage(null), null)
})

t('畸形 batch 返回 null 而不是抛', () => {
  eq(parseScheduleMessage('[SCHEDULE REMINDER BATCH]\nreminders_json: 不是json'), null)
  eq(parseScheduleMessage('[SCHEDULE REMINDER BATCH]\nreminders_json: {"a":1}'), null)
})

const BATCH = `[SCHEDULE REMINDER BATCH]
This is a scheduled message from the user
reminders_json: [{"schedule_id":"s1","occurrence_at":"2026-10-22T04:00:00.000Z","reminder_prompt":"A"},{"schedule_id":"s2","occurrence_at":"2026-10-22T04:10:00.000Z","reminder_prompt":"B"}]`

t('解析批量框架', () => {
  const p = parseScheduleMessage(BATCH)
  eq(p.batch, true)
  eq(p.items.length, 2)
  eq(p.items[1].scheduleId, 's2')
  eq(p.items[1].prompt, 'B')
})

t('过期判定：全新鲜保留', () => {
  const now = Date.parse('2026-10-22T04:31:00.000Z')
  const p = parseScheduleMessage(SINGLE)
  eq(decideDisposition({ parsed: p, now, staleSkipMs: 600000 }), 'keep')
})

t('过期判定：整条过期丢弃', () => {
  const now = Date.parse('2026-10-22T05:00:00.000Z') // 迟到 30 分钟
  const p = parseScheduleMessage(SINGLE)
  eq(decideDisposition({ parsed: p, now, staleSkipMs: 600000 }), 'drop')
})

t('过期判定：部分过期裁剪', () => {
  const now = Date.parse('2026-10-22T04:20:00.000Z') // s1 迟到20min, s2 迟到10min
  const p = parseScheduleMessage(BATCH)
  eq(decideDisposition({ parsed: p, now, staleSkipMs: 900000 }), 'trim')
})

t('过期判定：阈值0关闭检查', () => {
  const now = Date.parse('2030-01-01T00:00:00.000Z')
  const p = parseScheduleMessage(SINGLE)
  eq(decideDisposition({ parsed: p, now, staleSkipMs: 0 }), 'keep')
})

t('时间不可解析时不当作过期', () => {
  const bad = SINGLE.replace('2026-10-22T04:30:21.585Z', 'not-a-date')
  const p = parseScheduleMessage(bad)
  const cls = classifyStaleness(p, Date.now(), 600000)
  eq(cls[0].stale, false)
  eq(cls[0].ageMs, null)
})

t('裁剪后只保留新鲜项且可再次解析', () => {
  const now = Date.parse('2026-10-22T04:20:00.000Z')
  const p = parseScheduleMessage(BATCH)
  const cls = classifyStaleness(p, now, 900000)
  const rebuilt = rebuildTrimmedMessage(cls, { id:'m1', source:{kind:'schedule'}, content:[{type:'text',text:BATCH}] })
  const again = parseScheduleMessage(messageText(rebuilt))
  eq(again.items.length, 1)
  eq(again.items[0].scheduleId, 's2')
  eq(rebuilt.id, 'm1')
  eq(rebuilt.source.kind, 'schedule')
})

t('findSuperseded：撤更旧的', () => {
  const now = Date.parse('2026-10-22T04:31:00.000Z')
  const older = { id:'old', source:{kind:'schedule'}, content:[{type:'text',text:SINGLE.replace('2026-10-22T04:30:21.585Z','2026-10-22T04:28:00.000Z')}] }
  const v = findSuperseded([older], 'schedule-52512284-a74d-44b1-9f74-b2a347c87003', '2026-10-22T04:30:21.585Z', { dedupeWindowMs: 600000, now })
  eq(v, ['old'])
})

t('findSuperseded：不撤更新的', () => {
  const now = Date.parse('2026-10-22T04:31:00.000Z')
  const newer = { id:'new', source:{kind:'schedule'}, content:[{type:'text',text:SINGLE.replace('2026-10-22T04:30:21.585Z','2026-10-22T04:35:00.000Z')}] }
  const v = findSuperseded([newer], 'schedule-52512284-a74d-44b1-9f74-b2a347c87003', '2026-10-22T04:30:21.585Z', { dedupeWindowMs: 600000, now })
  eq(v, [])
})

t('findSuperseded：窗口外不合并', () => {
  const now = Date.parse('2026-10-22T04:31:00.000Z')
  const old = { id:'old', source:{kind:'schedule'}, content:[{type:'text',text:SINGLE.replace('2026-10-22T04:30:21.585Z','2026-10-22T03:00:00.000Z')}] }
  const v = findSuperseded([old], 'schedule-52512284-a74d-44b1-9f74-b2a347c87003', '2026-10-22T04:30:21.585Z', { dedupeWindowMs: 600000, now })
  eq(v, [])
})

t('findSuperseded：不同 scheduleId 不误伤', () => {
  const now = Date.parse('2026-10-22T04:31:00.000Z')
  const other = { id:'x', source:{kind:'schedule'}, content:[{type:'text',text:SINGLE.replace('schedule-52512284-a74d-44b1-9f74-b2a347c87003','schedule-OTHER').replace('2026-10-22T04:30:21.585Z','2026-10-22T04:29:00.000Z')}] }
  const v = findSuperseded([other], 'schedule-52512284-a74d-44b1-9f74-b2a347c87003', '2026-10-22T04:30:21.585Z', { dedupeWindowMs: 600000, now })
  eq(v, [])
})

t('sourceKindOf', () => {
  eq(sourceKindOf({source:{kind:'schedule'}}), 'schedule')
  eq(sourceKindOf({source:{kind:'user'}}), 'user')
  eq(sourceKindOf({}), undefined)
})

console.log(`\n${pass} 通过, ${fail} 失败`)
process.exit(fail ? 1 : 0)
