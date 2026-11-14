"""可靠执行原语：数据库是事实源，Redis 队列只是唤醒器。

- 认领(claim)与发布(publish)都是带条件的原子 UPDATE，attempt 作为 fencing 代次；
  超时接管后旧执行者迟到的提交会因 attempt 失配被拒绝。
- 发布把 状态+底稿(result)+审计 放进同一事务，完成状态与底稿永远一致。
- 对账(reconcile)扫描「落库未入队 / 队列丢消息」(queued 超时) 与
  「执行者崩溃 / 求解后未发布」(running 租约过期) 的行并补投，重复投递由认领守卫吸收。
"""
import logging,os,time
from datetime import datetime
from redis import Redis
from sqlalchemy import and_,inspect,or_,text,update
from ..config.settings import REDIS_URL
from ..constants.status import RunStatus
from ..models import AttributionRun,BudgetScenario
from ..utils.clock import now
from .audit_service import log

JOB_QUEUE="journey:jobs"
MODELS={"run":AttributionRun,"scenario":BudgetScenario}
LEASE_SECONDS=float(os.environ.get("EXECUTOR_LEASE_SECONDS","30"))
REQUEUE_AFTER_SECONDS=float(os.environ.get("EXECUTOR_REQUEUE_AFTER_SECONDS","10"))
RECONCILE_INTERVAL_SECONDS=float(os.environ.get("EXECUTOR_RECONCILE_INTERVAL_SECONDS","5"))
queue=Redis.from_url(REDIS_URL,decode_responses=True)

def enqueue(queue_obj,kind,oid):
    if kind not in MODELS:raise ValueError("未知任务类型:"+str(kind))
    queue_obj.rpush(JOB_QUEUE,f"{kind}:{oid}")

def claim(db,kind,oid,owner,lease_seconds=LEASE_SECONDS):
    """原子认领：queued 可认领；running 仅当租约已过期(超时接管)。成功返回新代次的行。"""
    model=MODELS[kind];now_ts=time.time()
    stmt=(update(model).where(model.id==oid,or_(model.status==RunStatus.QUEUED,
        and_(model.status==RunStatus.RUNNING,model.lease_expires_at<now_ts)))
        .values(status=RunStatus.RUNNING,attempt=model.attempt+1,lease_owner=owner,
                lease_expires_at=now_ts+lease_seconds,updated_at=now(),error=""))
    if db.execute(stmt).rowcount!=1:db.rollback();return None
    db.commit();return db.get(model,oid)

def renew_lease(db,kind,oid,attempt,lease_seconds=LEASE_SECONDS):
    """按代次续约；代次失配(已被接管)时返回 False，旧执行者无法续上已丢失的租约。"""
    model=MODELS[kind]
    res=db.execute(update(model).where(model.id==oid,model.attempt==attempt,model.status==RunStatus.RUNNING)
        .values(lease_expires_at=time.time()+lease_seconds))
    db.commit();return res.rowcount==1

def publish_result(db,kind,oid,attempt,result):
    """fencing 发布：仅当仍是当前代次且处于 running 才生效；状态+底稿+审计同事务提交。"""
    model=MODELS[kind]
    res=db.execute(update(model).where(model.id==oid,model.attempt==attempt,model.status==RunStatus.RUNNING)
        .values(status=RunStatus.COMPLETED,result=result,error="",lease_owner="",lease_expires_at=0.0,updated_at=now()))
    if res.rowcount!=1:
        db.rollback();log(db,"worker",kind+".fenced",oid);db.commit()
        logging.warning("%s %s attempt %s fenced: 迟到结果已丢弃",kind,oid,attempt)
        return False
    log(db,"worker",kind+".completed",oid);db.commit();return True

def publish_failure(db,kind,oid,attempt,error):
    model=MODELS[kind]
    res=db.execute(update(model).where(model.id==oid,model.attempt==attempt,model.status==RunStatus.RUNNING)
        .values(status=RunStatus.FAILED,error=(error or "求解失败")[:500],lease_owner="",lease_expires_at=0.0,updated_at=now()))
    if res.rowcount!=1:
        db.rollback();log(db,"worker",kind+".fenced",oid);db.commit();return False
    log(db,"worker",kind+".failed",oid);db.commit();return True

def _age_seconds(row,now_ts):
    stamp=row.updated_at or row.created_at
    try:return now_ts-datetime.fromisoformat(stamp).timestamp()
    except (TypeError,ValueError):return float("inf")

def find_stuck(db,now_ts=None,requeue_after=REQUEUE_AFTER_SECONDS):
    """落库未入队/队列丢消息(queued 超时) + 执行者崩溃/求解后未发布(running 租约过期)。"""
    now_ts=now_ts or time.time();stuck=[]
    for kind,model in MODELS.items():
        for row in db.query(model).filter(model.status==RunStatus.RUNNING,model.lease_expires_at<now_ts):
            stuck.append((kind,row.id,"lease-expired"))
        for row in db.query(model).filter(model.status==RunStatus.QUEUED):
            if _age_seconds(row,now_ts)>=requeue_after:stuck.append((kind,row.id,"queued-stale"))
    return stuck

def reconcile(db,queue_obj,now_ts=None,requeue_after=REQUEUE_AFTER_SECONDS):
    """把卡住的行重新入队；重复投递无害(认领是条件原子操作)。返回各类恢复数量。"""
    stats={"lease-expired":0,"queued-stale":0,"errors":0}
    for kind,oid,reason in find_stuck(db,now_ts,requeue_after):
        try:
            enqueue(queue_obj,kind,oid);log(db,"worker",kind+".recovered",oid);db.commit();stats[reason]+=1
        except Exception:
            db.rollback();stats["errors"]+=1;logging.exception("reconcile enqueue failed: %s %s",kind,oid)
    return stats

EXECUTION_COLUMNS=[("attempt","INTEGER NOT NULL DEFAULT 0"),("lease_owner","VARCHAR NOT NULL DEFAULT ''"),
    ("lease_expires_at","DOUBLE PRECISION NOT NULL DEFAULT 0"),("updated_at","VARCHAR")]

def ensure_schema(engine):
    """幂等补齐执行列，与 migrations/002_reliable_execution.sql 等价；已有数据(含获批底稿)不动。"""
    with engine.begin() as conn:
        insp=inspect(conn)
        for table in ("budget_scenarios","attribution_runs"):
            if not insp.has_table(table):continue
            existing={c["name"] for c in insp.get_columns(table)}
            for name,ddl in EXECUTION_COLUMNS:
                if name not in existing:conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))
            conn.execute(text(f"UPDATE {table} SET updated_at=created_at WHERE updated_at IS NULL"))
