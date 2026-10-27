/**
 * dsh-cron-guard —— 定时唤醒治理（投递插队 / 过期作废 / 同源合并 / 审批超时）
 *
 * ## 为什么要这个插件
 *
 * DSH 的定时唤醒走 `dsh-schedule` 的 `drive()`，它唯一的投递语句是
 * `agent.followup(message)` —— 排进收件箱的 `next-turn` 队列，必须等当前
 * **整个 turn**（所有剩余 step 加 turn-stopping 钩子）跑完才被领取。
 * 本机实测：BOSS 会话 turn 中位 166s、p90 1542s，唤醒 p90 等待 1862 秒。
 *
 * 但 agent-loop 的 step 中位数只有 11.9 秒。所以把投递目标从 `next-turn`
 * 换成 `next-step`，等待时间就差一个数量级。
 *
 * 不修改 `app.asar` 的做法：`agent/inbox/inserted` 事件在 `send()` 内部
 * **先于** `wakeDriver()` 同步触发（见 dsh-agent-loop 的 `send()`：
 * `this.inbox.splice(...)` 然后 `if (wakeup) this.wakeDriver(...)`）。
 * 因此在这里重挂消息，可以在驱动器决定投递语义之前改写结果。
 *
 * ## 四件事
 *
 * 1. **插队**：忙碌时把 schedule 消息从 `next-turn` 搬到 `next-step`，
 *    下一步就领取，不必等整个 turn 结束。空闲时不搬（turn 首步本来就会
 *    把 `next-step` 全领走），行为与原生完全一致。
 * 2. **过期作废**：`occurrence_at` 距今超过 `staleSkipMs` 的唤醒直接丢弃，
 *    切断「堆积 → 每条都跑完整流程 → 越堆越多」的正反馈。
 * 3. **同源合并**：同一 `schedule_id` 如果已有一条更新的待投递唤醒，
 *    旧的直接撤掉，只留最新一条（对齐 SQS 同 MessageGroupId 覆盖、
 *    Kafka compaction、BullMQ dedupe 的语义）。
 * 4. **审批超时**：`approval/request` 超过 `approvalTimeoutMs` 未决，
 *    按 `rejected` 收口。这是那起 8.2 小时卡死的直接原因——会话一直
 *    显示"正在运行"，其实在等人点按钮，后面所有唤醒全堵在门口。
 *
 * 所有动作都写日志，默认 `dryRun: false`。可先开 `dryRun` 观察一轮。
 */

export const name = 'dsh-cron-guard'

/** 只依赖可选服务，全部走 ctx.get()，缺了降级而不是不激活。 */
export const inject = []

const LOG_PREFIX = '[cron-guard]'

/** 单条 schedule 消息里能容纳的最大批量提醒数量，防御畸形输入。 */
const MAX_BATCH_ITEMS = 64

/**
 * 默认配置。注意这里用的是普通对象默认值，宿主会用 Schemastery 覆盖；
 * 没装 Schemastery 时也能跑，保持零硬依赖。
 */
export const DEFAULT_CONFIG = {
  /** 总开关。 */
  enabled: true,
  /** 只观察不动作，用于上线前验证解析是否正确。 */
  dryRun: false,
  /**
   * 忙碌时把 schedule 投递从 next-turn 改投 next-step。
   * 这是本插件收益最大的一项，关闭后其余三项仍生效。
   */
  relocateToNextStep: true,
  /** 唤醒迟到超过这个毫秒数就丢弃。0 表示不做过期检查。 */
  staleSkipMs: 10 * 60 * 1000,
  /** 同一 schedule_id 的合并窗口；0 表示不做合并。 */
  dedupeWindowMs: 10 * 60 * 1000,
  /** 审批超过这个毫秒数未决就按 rejected 收口。0 表示不介入审批。 */
  approvalTimeoutMs: 15 * 60 * 1000,
  /** 只对这些来源的消息生效；schedule 是唯一内置值。 */
  sourceKinds: ['schedule'],
  /**
   * 让路给用户：`next-turn` 里还有用户消息时，不把唤醒插到它前面。
   *
   * `claim()` 先领 `next-step` 再领 `next-turn`，所以改投 next-step 等于
   * 插到排队中的用户消息前面。自动化永远不该抢在用户打字之前。
   */
  yieldToUser: true,
}

/* ------------------------------------------------------------------ *
 * 纯函数区：解析 / 判定。全部可单测，不碰 ctx。
 * ------------------------------------------------------------------ */

/**
 * 解析一条 schedule 唤醒消息的文本。
 *
 * 两种框架由 dsh-schedule 生成（见其 `renderReminderFraming` 与
 * `renderRecurringReminderBatchFraming`）：
 *
 *   [SCHEDULE REMINDER]
 *   This is a scheduled message from the user
 *   schedule_id_json: "schedule-..."
 *   occurrence_at: 2026-10-22T03:12:03.940Z
 *   reminder_prompt_json: "..."
 *
 *   [SCHEDULE REMINDER BATCH]
 *   This is a scheduled message from the user
 *   reminders_json: [{"schedule_id":"...","occurrence_at":"...","reminder_prompt":"..."}]
 *
 * @param {string} text - 消息拼接后的完整文本。
 * @returns {{batch: boolean, items: Array<{scheduleId: string, occurrenceAt: string, prompt: string}>}|null}
 *   解析失败返回 null（调用方必须原样放行，绝不因为解析不了就丢消息）。
 */
export function parseScheduleMessage(text) {
  if (typeof text !== 'string' || text.length === 0) return null
  const head = text.slice(0, 64)
  try {
    if (head.includes('[SCHEDULE REMINDER BATCH]')) {
      const raw = fieldValue(text, 'reminders_json')
      if (raw === undefined) return null
      const list = JSON.parse(raw)
      if (!Array.isArray(list)) return null
      const items = []
      for (const entry of list.slice(0, MAX_BATCH_ITEMS)) {
        if (!entry || typeof entry !== 'object') continue
        if (typeof entry.schedule_id !== 'string') continue
        items.push({
          scheduleId: entry.schedule_id,
          occurrenceAt: typeof entry.occurrence_at === 'string' ? entry.occurrence_at : '',
          prompt: typeof entry.reminder_prompt === 'string' ? entry.reminder_prompt : '',
        })
      }
      return items.length === 0 ? null : { batch: true, items }
    }
    if (head.includes('[SCHEDULE REMINDER]')) {
      const idRaw = fieldValue(text, 'schedule_id_json')
      if (idRaw === undefined) return null
      const scheduleId = JSON.parse(idRaw)
      if (typeof scheduleId !== 'string') return null
      const occurrenceRaw = fieldValue(text, 'reminder_prompt_json')
      return {
        batch: false,
        items: [
          {
            scheduleId,
            occurrenceAt: (fieldValue(text, 'occurrence_at') ?? '').trim(),
            prompt: occurrenceRaw === undefined ? '' : safeJsonString(occurrenceRaw),
          },
        ],
      }
    }
  } catch {
    return null
  }
  return null
}

/** 取 `key: value` 形式的整行值（value 到行尾）。 */
function fieldValue(text, key) {
  const marker = `${key}: `
  const at = text.indexOf(marker)
  if (at < 0) return undefined
  const from = at + marker.length
  const end = text.indexOf('\n', from)
  return end < 0 ? text.slice(from) : text.slice(from, end)
}

/** JSON 字符串字段尽力解出原文；解不出就当空串，不影响其余判定。 */
function safeJsonString(raw) {
  try {
    const parsed = JSON.parse(raw)
    return typeof parsed === 'string' ? parsed : ''
  } catch {
    return ''
  }
}

/** 从消息对象里拼出全部文本内容。 */
export function messageText(message) {
  const parts = message?.content
  if (!Array.isArray(parts)) return ''
  let out = ''
  for (const part of parts) {
    if (part && part.type === 'text' && typeof part.text === 'string') out += part.text
  }
  return out
}

/** 消息来源种类，非 schedule 返回 undefined。 */
export function sourceKindOf(message) {
  const kind = message?.source?.kind
  return typeof kind === 'string' ? kind : undefined
}

/**
 * 判断一条消息里哪些提醒已经过期。
 *
 * @param {{batch:boolean,items:Array}} parsed - parseScheduleMessage 的结果。
 * @param {number} now - 当前毫秒时间戳。
 * @param {number} staleSkipMs - 允许的最大迟到时长；<=0 关闭检查。
 * @returns {Array} 逐条判定，`stale` 为 true 表示可以丢。
 */
export function classifyStaleness(parsed, now, staleSkipMs) {
  return parsed.items.map((item) => {
    const at = Date.parse(item.occurrenceAt)
    if (!Number.isFinite(at)) return { ...item, stale: false, ageMs: null }
    const ageMs = now - at
    return { ...item, ageMs, stale: staleSkipMs > 0 && ageMs > staleSkipMs }
  })
}

/**
 * 决定一条 schedule 消息该怎么处理。
 *
 * @param {object} input
 * @param {{batch:boolean,items:Array}} input.parsed - 解析结果。
 * @param {number} input.now - 当前毫秒时间戳。
 * @param {number} input.staleSkipMs - 过期阈值。
 * @returns {'keep'|'drop'|'trim'} 整条保留 / 整条丢弃 / 部分过期需裁剪。
 */
export function decideDisposition({ parsed, now, staleSkipMs }) {
  if (staleSkipMs <= 0) return 'keep'
  const classified = classifyStaleness(parsed, now, staleSkipMs)
  const staleCount = classified.filter((item) => item.stale).length
  if (staleCount === 0) return 'keep'
  if (staleCount === classified.length) return 'drop'
  return 'trim'
}

/**
 * 裁剪掉过期的批量提醒，重建一条只含新鲜项的消息文本。
 *
 * 只处理 batch 框架；单条框架的过期已在 disposition 阶段变成 drop。
 *
 * @param {Array} classified - classifyStaleness 的结果。
 * @param {object} original - 原消息，用于继承 id 以外的一切字段。
 * @returns {object} 新消息对象（保留 source，正文重建）。
 */
export function rebuildTrimmedMessage(classified, original) {
  const fresh = classified.filter((item) => !item.stale)
  const payload = fresh.map((item) => ({
    schedule_id: item.scheduleId,
    occurrence_at: item.occurrenceAt,
    reminder_prompt: item.prompt,
  }))
  const text = [
    '[SCHEDULE REMINDER BATCH]',
    'This is a scheduled message from the user',
    `reminders_json: ${JSON.stringify(payload)}`,
  ].join('\n')
  return {
    ...original,
    content: [
      ...(Array.isArray(original.content)
        ? original.content.filter((part) => part?.type !== 'text')
        : []),
      { type: 'text', text },
    ],
  }
}

/**
 * 在待投递消息里找同源且更旧的副本，用于合并。
 *
 * @param {Array<object>} pending - 待投递消息（next-turn 与 next-step 合并）。
 * @param {string} scheduleId - 基准 schedule_id。
 * @param {string} newerOccurrenceAt - 本次消息的 occurrence_at。
 * @param {object} options
 * @param {number} options.dedupeWindowMs - 合并窗口；<=0 关闭。
 * @param {number} options.now - 当前毫秒时间戳。
 * @returns {Array<string>} 应当撤掉的更旧消息 id。
 */
export function findSuperseded(pending, scheduleId, newerOccurrenceAt, { dedupeWindowMs, now }) {
  if (dedupeWindowMs <= 0) return []
  const newerAt = Date.parse(newerOccurrenceAt)
  if (!Number.isFinite(newerAt)) return []
  const victims = []
  for (const message of pending) {
    if (sourceKindOf(message) !== 'schedule') continue
    const parsed = parseScheduleMessage(messageText(message))
    if (parsed === null) continue
    const hit = parsed.items.find((item) => item.scheduleId === scheduleId)
    if (hit === undefined) continue
    const at = Date.parse(hit.occurrenceAt)
    if (!Number.isFinite(at)) continue
    // 只撤更旧的；同刻或更新的原样保留，避免把新消息挤掉。
    if (at >= newerAt) continue
    // 窗口外的不动：太久以前的属于另一段上下文，粗暴合并会丢信息。
    if (now - at > dedupeWindowMs) continue
    victims.push(message.id)
  }
  return victims
}

/* ------------------------------------------------------------------ *
 * 运行时：把上面的判定接到真实事件上。
 * ------------------------------------------------------------------ */

/**
 * `next-turn` 里是否有排队中的用户消息。
 *
 * 有的话唤醒就不该抢到前面去：用户敲的字永远优先于定时任务。
 *
 * @param {Array<object>} nextTurn - 当前排队等待下一轮的待投递消息。
 * @returns {boolean}
 */
export function hasQueuedUserMessage(nextTurn) {
  return nextTurn.some((message) => sourceKindOf(message) === 'user')
}

/**
 * 插件入口。
 * @param {import('@deepseek-ai/cordis').Context} ctx - 宿主上下文。
 * @param {object} userConfig - cordis.patch.yml 里的 config。
 */
export function apply(ctx, userConfig = {}) {
  const config = { ...DEFAULT_CONFIG, ...userConfig }
  const log = (message) => {
    try {
      ctx.logger?.info?.(`${LOG_PREFIX} ${message}`)
    } catch {
      /* 日志失败不能影响主流程 */
    }
  }
  if (!config.enabled) {
    log('已禁用，不启动')
    return
  }

  const stats = {
    inserted: 0,
    relocated: 0,
    dropped: 0,
    trimmed: 0,
    superseded: 0,
    approvalTimedOut: 0,
  }

  /**
   * 正在被本插件重挂的消息 id。
   *
   * 重挂会再触发一次 `agent/inbox/inserted`，没有这个护栏就会无限递归。
   */
  const busy = new Set()

  ctx.effect(() => {
    const offInserted = ctx.on('agent/inbox/inserted', ({ agent, message }) => {
      if (agent === undefined || message === undefined) return
      if (sourceKindOf(message) !== 'schedule') {
        // 非 schedule 来源不在治理范围；但仍然数一下便于观察。
        return
      }
      if (busy.has(message.id)) return
      stats.inserted += 1

      const inbox = agent.inbox
      if (inbox === undefined) return

      const now = Date.now()

      /**
       * 经过治理后真正要投递的消息。
       *
       * 裁剪会重建消息对象；如果后面搬移还用原来的 `message`，裁剪结果
       * 就会被丢掉、过期内容被原样搬回去。所有改动都必须落到这个变量上。
       */
      let effective = message

      /* ---- 1. 过期作废 ---- */
      const parsed = parseScheduleMessage(messageText(message))
      // 解析不了就放行。宁可漏治一条，也不能丢用户的定时任务。
      if (parsed !== null) {
        const disposition = decideDisposition({
          parsed,
          now,
          staleSkipMs: config.staleSkipMs,
        })
        if (disposition === 'drop') {
          const ages = classifyStaleness(parsed, now, config.staleSkipMs)
            .map((item) => item.ageMs)
            .filter((age) => age !== null)
          const worst = ages.length === 0 ? 0 : Math.max(...ages)
          if (config.dryRun) {
            log(`[dry-run] 将丢弃过期唤醒 ${parsed.items[0]?.scheduleId}（迟到 ${Math.round(worst / 1000)}s）`)
          } else {
            busy.add(message.id)
            try {
              inbox.remove(message.id)
            } finally {
              busy.delete(message.id)
            }
            stats.dropped += 1
            log(`丢弃过期唤醒 ${parsed.items[0]?.scheduleId}（迟到 ${Math.round(worst / 1000)}s）`)
            return
          }
        } else if (disposition === 'trim') {
          const classified = classifyStaleness(parsed, now, config.staleSkipMs)
          const kept = classified.filter((item) => !item.stale).length
          if (config.dryRun) {
            log(`[dry-run] 将裁剪批量唤醒：保留 ${kept}/${classified.length}`)
          } else {
            const rebuilt = rebuildTrimmedMessage(classified, message)
            busy.add(message.id)
            try {
              inbox.replace(message.id, rebuilt)
            } finally {
              busy.delete(message.id)
            }
            // 后续搬移必须投递裁剪后的内容，不能把原消息搬回去。
            effective = rebuilt
            stats.trimmed += 1
            log(`裁剪批量唤醒：保留 ${kept}/${classified.length}`)
          }
        }

        /* ---- 2. 同源合并 ---- */
        if (config.dedupeWindowMs > 0 && disposition !== 'drop') {
          for (const item of parsed.items) {
            const pending = [...inbox.nextTurn, ...inbox.nextStep]
            const victims = findSuperseded(pending, item.scheduleId, item.occurrenceAt, {
              dedupeWindowMs: config.dedupeWindowMs,
              now,
            })
            for (const victimId of victims) {
              if (victimId === message.id) continue
              if (config.dryRun) {
                log(`[dry-run] 将合并掉更旧的唤醒 ${item.scheduleId}`)
                continue
              }
              busy.add(victimId)
              try {
                inbox.remove(victimId)
              } finally {
                busy.delete(victimId)
              }
              stats.superseded += 1
            }
          }
        }
      }

      /* ---- 3. 插队：忙碌时改投 next-step ---- */
      if (!config.relocateToNextStep) return
      if (agent.status !== 'running') return
      // 用户消息永远优先。用户已经在排队等着说话了，就别让定时任务插队。
      const queuedUser = config.yieldToUser && hasQueuedUserMessage(inbox.nextTurn)
      if (queuedUser) {
        log(`暂不改投 ${message.id}：用户消息正在排队，先让路`)
        return
      }
      if (config.dryRun) {
        log(`[dry-run] 将把 ${message.id} 改投 next-step（会话忙）`)
        return
      }
      if (!inbox.nextTurn.some((item) => item.id === effective.id)) return
      busy.add(effective.id)
      try {
        inbox.remove(effective.id)
        const stillThere = inbox.nextTurn.some((item) => item.id === effective.id)
        if (!stillThere) {
          try {
            inbox.append('next-step', effective)
          } catch (error) {
            // 移除成功但追加失败：消息已经离开 next-turn，再不补回去就彻底丢了。
            // 宁可退回原队列（等于没治理），也不能丢用户的定时任务。
            log(`改投失败，回退到 next-turn：${String(error)}`)
            try {
              inbox.append('next-turn', effective)
            } catch (fallbackError) {
              log(`回退也失败，消息 ${effective.id} 可能丢失：${String(fallbackError)}`)
            }
            return
          }
        }
      } finally {
        busy.delete(effective.id)
      }
      stats.relocated += 1
      log(`改投 next-step ${message.id}（会话忙，不必等整个 turn）`)
    }, { prepend: true })

    /* ---- 4. 审批超时 ---- */
    const offApproval =
      config.approvalTimeoutMs > 0
        ? ctx.on('approval/request', async (request, next) => {
            const timeoutMs = config.approvalTimeoutMs
            const started = Date.now()
            let timer
            const timedOut = new Promise((resolve) => {
              // 不 unref：unref 过的定时器在事件循环没有别的工作时会被跳过，
              // 这一轮就永远不会超时——正好退回我们要消灭的那个卡死状态。
              // 宿主是常驻进程，多一个挂着的定时器无害。
              timer = setTimeout(() => resolve('rejected'), timeoutMs)
            })
            try {
              const outcome = await Promise.race([Promise.resolve(next()), timedOut])
              if (outcome === 'rejected' && Date.now() - started >= timeoutMs) {
                stats.approvalTimedOut += 1
                log(
                  `审批超时 ${Math.round(timeoutMs / 60000)} 分钟，按 rejected 收口` +
                    `（工具 ${request?.toolName ?? '?'}）；会话不再空转`,
                )
              }
              return outcome
            } finally {
              clearTimeout(timer)
            }
          }, { prepend: true })
        : () => {}

    return () => {
      offInserted()
      offApproval()
    }
  }, 'dsh-cron-guard lifecycle')

  // 统计挂在模块级而不是 ctx.set()：ctx.set() 要求该服务已被 provide，
  // 否则直接抛错、插件加载失败。这里只想让人能读到计数，不值得注册服务。
  lastStats = stats
}

/**
 * 最近一次 apply() 的运行时统计。
 *
 * 供调试与自检读取，不是稳定的服务接口。
 */
export let lastStats = null
