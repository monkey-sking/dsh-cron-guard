import { apply } from '../lib/index.js'

let pass=0, fail=0
const t=(n,f)=>{try{const r=f(); if(r?.then){return r.then(()=>{pass++;console.log('  ok  ',n)},e=>{fail++;console.log('  FAIL',n,'\n       ',e.message)})} pass++;console.log('  ok  ',n)}catch(e){fail++;console.log('  FAIL',n,'\n       ',e.message)}}
const eq=(a,b,m)=>{if(JSON.stringify(a)!==JSON.stringify(b))throw new Error(`${m||''} 期望 ${JSON.stringify(b)} 实得 ${JSON.stringify(a)}`)}

function makeCtx(){
  const listeners=[]
  const logs=[]
  const ctx={
    logger:{info:m=>logs.push(m)},
    effect:fn=>fn(),
    on:(name,fn)=>{ if(name==='approval/request') listeners.push(fn); return ()=>{} },
    set:()=>{},
  }
  const call = async (req, next) => {
    // 复现 cordis waterfall：prepend 的监听器最先拿到 next
    let chain = next
    for (const l of [...listeners].reverse()) {
      const inner = chain
      chain = () => l(req, inner)
    }
    return chain()
  }
  return { ctx, logs, call, count: ()=>listeners.length }
}

await t('审批正常放行：不干扰原结果', async () => {
  const w = makeCtx()
  apply(w.ctx, { approvalTimeoutMs: 60000 })
  const out = await w.call({ toolName:'bash' }, async () => 'allowed-once')
  eq(out, 'allowed-once')
})

await t('审批被拒：原样透传 rejected', async () => {
  const w = makeCtx()
  apply(w.ctx, { approvalTimeoutMs: 60000 })
  const out = await w.call({ toolName:'bash' }, async () => 'rejected')
  eq(out, 'rejected')
})

await t('审批超时：按 rejected 收口，不再永久挂起', async () => {
  const w = makeCtx()
  apply(w.ctx, { approvalTimeoutMs: 120 })
  const started = Date.now()
  const out = await w.call({ toolName:'bash' }, () => new Promise(()=>{})) // 永不 resolve
  const took = Date.now() - started
  eq(out, 'rejected')
  if (took > 2000) throw new Error(`超时保护太慢：${took}ms`)
  if (!w.logs.some(l=>l.includes('审批超时'))) throw new Error('没有记日志')
})

await t('approvalTimeoutMs:0 完全不介入审批', async () => {
  const w = makeCtx()
  apply(w.ctx, { approvalTimeoutMs: 0 })
  eq(w.count(), 0, '不应注册监听器')
})

await t('下游抛错时仍然清理定时器且透传错误', async () => {
  const w = makeCtx()
  apply(w.ctx, { approvalTimeoutMs: 60000 })
  let threw=false
  try { await w.call({ toolName:'bash' }, async () => { throw new Error('boom') }) }
  catch(e){ threw = e.message==='boom' }
  if(!threw) throw new Error('错误没有透传')
})

await t('下游返回异常值不被吞掉', async () => {
  const w = makeCtx()
  apply(w.ctx, { approvalTimeoutMs: 60000 })
  const out = await w.call({ toolName:'bash' }, async () => 'unavailable')
  eq(out, 'unavailable')
})

console.log(`\n${pass} 通过, ${fail} 失败`)
process.exit(fail?1:0)
