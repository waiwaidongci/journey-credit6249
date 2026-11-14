import {StatusBadge} from './common/StatusBadge';
interface ExecutionRow{status:string;error:string;attempt:number;lease_owner:string;lease_expires_at:number}
/** 队列/执行恢复进度：尝试代次、执行者租约与对账接管提示，随页面轮询每2秒刷新。 */
export function ExecutionProgress({row,hint}:{row:ExecutionRow;hint:string}){
  const attempt=Math.max(1,row.attempt||0),recovered=row.attempt>1;
  const leaseLeft=row.status==='running'&&row.lease_expires_at?Math.max(0,Math.round(row.lease_expires_at-Date.now()/1000)):0;
  return <div className="pending"><StatusBadge status={row.status}/>
    {row.error?<p>{row.error}</p>:<>
      <p>{hint}</p>
      <p className="recovery-line">第 {attempt} 次执行{recovered?' · 原执行者超时，已自动接管恢复':''}</p>
      {row.status==='running'&&leaseLeft>0&&<p className="caption">执行者 {row.lease_owner||'worker'} 计算中并周期续约；若 {leaseLeft} 秒内失联，对账器将自动接管。</p>}
      {row.status==='running'&&leaseLeft<=0&&<p className="caption">执行者租约已到期，对账器正在接管重投，无需重新提交。</p>}
      {row.status==='queued'&&<p className="caption">已落库入队；若队列消息丢失，对账器会在数秒内自动补投。</p>}
    </>}
  </div>;
}
