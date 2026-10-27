# dsh-cron-guard

治理 DSH 的定时唤醒：**不必等当前对话跑完**，同时丢掉过期的、合并重复的、给卡住的审批收口。

## 它解决什么

DSH 的定时任务（`dsh-schedule`）唯一投递语句是 `agent.followup(message)`，排进收件箱的 **`next-turn`** 队列——必须等当前 **整个 turn**（所有剩余 step + turn-stopping 钩子）跑完才被领取。

本机 4 天实测（`~/.dsh/sessions/*/*/session.v4.jsonl.zstd` 全量回放，555 次唤醒）：

| 会话 | 唤醒中位等待 | p90 | 最长 | >5 分钟的占比 |
|---|---|---|---|---|
| BOSS | 12s | **1862s** | **94 分钟** | **34.2%** |
| X 主力 | 0s | 98s | 21.6 分钟 | 6.0% |

而同机的 **step 中位数只有 11.9 秒**。等一个 step 和等一整个 turn 差一个数量级。

按历史事件重放测算，只改投递目标一项：

| 会话 | 现状 p90 | 改后 p90 | 效果 |
|---|---|---|---|
| BOSS | 1862s | **168s** | 快 11× |
| X 主力 | 98s | **17s** | 快 5.8× |
| BOSS 旧 | 684s | **30s** | 快 22.8× |

## 为什么不用改 `app.asar`

`dsh-agent-loop` 的 `send()` 是这样的：

```js
send(message, target, wakeup) {
  const wakingAfterAbort = ...
  this.inbox.splice(resolvedTarget, Infinity, 0, [message]);   // ← 这里同步发出 agent/inbox/inserted
  if (wakeup) this.wakeDriver(wakingAfterAbort);               // ← 之后才决定投递语义
}
```

`inbox.splice()` 内部 `dispatch.emit("agent/inbox/inserted", ...)` **同步**执行。所以插件在这个事件里重挂消息，可以赶在驱动器动作之前改写结果。`Agent.inbox` 是公开的结构化接口，暴露 `append` / `prepend` / `replace` / `remove` / `clear` / `splice`。

## 四项治理

| 项 | 做什么 | 默认 |
|---|---|---|
| **插队** `relocateToNextStep` | 会话忙时把唤醒从 `next-turn` 搬到 `next-step`，下一步就领取 | 开 |
| **让路** `yieldToUser` | `next-turn` 里有用户消息时不插队——自动化不该抢在用户打字之前 | 开 |
| **过期作废** `staleSkipMs` | 迟到超 10 分钟的唤醒直接丢弃 | 10 分钟 |
| **同源合并** `dedupeWindowMs` | 同 `schedule_id` 只留最新一条（SQS 同组覆盖语义） | 10 分钟 |
| **审批超时** `approvalTimeoutMs` | 审批超 15 分钟按 `rejected` 收口 | 15 分钟 |

### 为什么「让路」是必需的

`claim(target, turn)` 先领 `next-step` 再领 `next-turn`：

```js
claim(target, turn) {
  const claimed = this.mutate("next-step", 0, this.nextStep.length, [], false);
  if (target === "next-turn") claimed.push(...this.mutate("next-turn", 0, 1, [], false));
  ...
}
```

用户消息默认走 `followup` → `next-turn`。所以把唤醒搬到 `next-step` 等于插到排队中的用户消息**前面**。`yieldToUser` 就是为了这个：用户已经在等，就别插队。

### 为什么「审批超时」最该先上

本机 10-23 那次事故：`approval/asked` 在 23:01:15（Auto review 拒绝了「agent 自选 7 个目标批量发首触」），用户睡了，`allowed-once` 到次日 07:24 才来——**会话空转 492 分钟**，期间 4 条唤醒排队。全量 247 次审批累计等待 **1303 分钟**。

「会话忙」有很大比例不是真在干活，是在等人点按钮。

## 安全边界

刻意做成「宁可漏治，不可误伤」：

- **解析不了的消息原样放行**，绝不因为格式不认识就丢你的定时任务。
- **防递归护栏**：重挂会再次触发 `inserted`，用 `busy` 集合挡住无限套娃。
- **用户消息永不搬动**，只处理 `source.kind === 'schedule'`。
- **只撤更旧的**：同 `schedule_id` 且 `occurrence_at` 更晚的不会被合并掉。
- **窗口外不合并**：太久以前的属于另一段上下文。
- **`dryRun: true`** 只记日志不动队列，可先观察一轮。

## 配置

```yaml
- id: cron-guard
  name: dsh-cron-guard
  config:
    enabled: true
    dryRun: false              # 先开 true 观察一轮
    relocateToNextStep: true   # 收益最大的一项
    yieldToUser: true          # 用户消息优先
    staleSkipMs: 600000        # 10 分钟；0 = 关
    dedupeWindowMs: 600000     # 10 分钟；0 = 关
    approvalTimeoutMs: 900000  # 15 分钟；0 = 关
    sourceKinds: [schedule]
```

## 测试

共 47 例，分四层：

```bash
cd test
node parse.test.mjs      # 15 例：解析、过期判定、裁剪、合并选择（纯函数）
node runtime.test.mjs    # 15 例：搬移时序、防递归、让路、dry-run、回归
node approval.test.mjs   #  6 例：审批超时、错误透传、定时器清理
node host.test.mjs       # 11 例：真实 cordis 装载、事件时序、降级、队列改写、失败回退
```

`host.test.mjs` 用**真实 cordis 运行时**（不是手搓 mock）装载插件，并按 `dsh-agent-loop` 里 `ReactLoopInbox` 的真实语义实现假 agent（`append` 同步触发 `inserted`、`remove` 真摘消息）。这样时序类 bug 能被测出来——实际上就是靠它抓到了四个真 bug：

1. 超时定时器 `unref()` 会在事件循环空闲时被跳过，正好退回要消灭的卡死状态。
2. 裁剪后若仍搬移原 `message` 对象，裁剪结果会丢失、过期内容被原样搬回去。
3. `ctx.set('cron-guard', ...)` 会因服务未 `provide` 抛错，导致插件**加载即失败**。
4. `remove()` 成功但 `append('next-step')` 抛错时，消息已离开 `next-turn` → **彻底丢失**。现在会回退到 `next-turn`，宁可白治理一次也不丢任务。

## 已知局限

- **不解决「一个会话里的长 turn」**。它把等待从「整个 turn」压到「一个 step」，但如果某个 step 本身就跑了 10 分钟（本机 p99 = 287s、max 36 分钟），那一步还是会等。真正根治要换会话架构（新会话执行 + 结果回流），那是另一件事。
- **审批超时按 `rejected` 收口**，意味着超时后该工具调用不会执行。如果那个操作很重要，得靠次日待办补，不能指望它自己重试。
- **只治理 `source.kind === 'schedule'`**。IM 消息、其他插件投递的唤醒不在范围内。
- **「空闲时不搬」是有意的**。`turn` 首步本来就会把 `next-step` 全领走，空闲时搬到 `next-step` 与原生行为等价，所以只在 `agent.status === 'running'` 时才动手——减少不必要的队列写。
- **审批超时后，界面上那条审批提示不会自己消失**。它仍在等你点，但点了也不会生效（`decide()` 已经把结果记成 `rejected`）。这是有意取舍：宁可留一个可忽略的残影，也不要让会话继续空转 8 小时。嫌碍眼就手动点掉。
- **审批超时只覆盖 `approval/request` 这一个入口**。如果某个工具调用卡在别的等待上（比如等网络、等子进程），不在此列。
- **搬移是一次「移除 + 追加」两次投影写入**，不是原子操作。失败时会回退到 `next-turn`，但回退本身也失败（磁盘满等）时消息仍可能丢，日志里会有 `回退也失败`。
- **合并窗口内的信息会丢**：同 `schedule_id` 的旧提醒被撤掉时，它携带的上下文（比如「发现 3 条新回复」）不会并入新提醒。如果提醒正文本身是增量信息，把 `dedupeWindowMs` 调小或设 0。

## 安装

```bash
# 1. 放进插件目录
ln -s "$PWD" ~/.local/share/dsh-cron-guard

# 2. 在 profile 的 package.json 里加依赖
#    "dsh-cron-guard": "link:/Users/<you>/.local/share/dsh-cron-guard"
#    并加进 dsh.profile.bundles

# 3. 重启 DSH
```
