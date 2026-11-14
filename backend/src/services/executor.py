"""执行者：认领→(续约)求解→fencing 发布。崩溃安全由 execution 原语与对账器保证。"""
import json,logging,socket,os,threading,time,traceback,uuid
from pathlib import Path
from types import SimpleNamespace
from ..models import AttributionRun,JourneyBatch
from .attribution_algorithm import analyze_attribution
from .budget_algorithm import optimize_budget
from .channel_service import list_channels
from .execution import (JOB_QUEUE,LEASE_SECONDS,RECONCILE_INTERVAL_SECONDS,claim,publish_failure,
    publish_result,reconcile,renew_lease)
from .journey_service import list_journeys
from ..utils.fingerprint import fingerprint

class Executor:
    def __init__(self,queue,session_factory,worker_id=None,lease_seconds=LEASE_SECONDS,artifacts="/artifacts"):
        self.queue=queue;self.session_factory=session_factory;self.lease_seconds=lease_seconds
        self.worker_id=worker_id or f"worker-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.artifacts=Path(artifacts)

    def load_inputs(self,db,kind,row):
        if kind=="run":
            batch=db.get(JourneyBatch,row.batch_id)
            if not batch:raise ValueError("路径批次不存在")
            return {"batch":SimpleNamespace(id=batch.id,name=batch.name,source=batch.source),
                    "journeys":list_journeys(db,batch.id),"channels":list_channels(db)}
        run=db.get(AttributionRun,row.run_id)
        if not run or run.status!="completed" or not run.result:raise ValueError("关联归因研究不存在或未完成")
        return {"attribution":run.result,"budget":row.budget,"capacity_overrides":row.capacity_overrides or {}}

    def solve(self,kind,payload):
        if kind=="run":return analyze_attribution(payload["batch"],payload["journeys"],payload["channels"])
        result=optimize_budget(payload["attribution"]["credits"],payload["budget"],payload["capacity_overrides"])
        result["input_hash"]=fingerprint({"method":"linear-association-budget-v1","attribution":payload["attribution"],
            "budget":payload["budget"],"capacity_overrides":payload["capacity_overrides"]})
        result["attribution_hash"]=payload["attribution"]["input_hash"]
        return result

    def process_message(self,message):
        """处理一条队列消息；重复投递/已终结的行由认领守卫吸收。返回是否实际执行。"""
        try:kind,raw_id=message.split(":",1);oid=int(raw_id)
        except (ValueError,AttributeError):logging.warning("忽略无法解析的队列消息: %r",message);return False
        if kind not in ("run","scenario"):return False
        with self.session_factory() as db:
            row=claim(db,kind,oid,self.worker_id,self.lease_seconds)
            if not row:return False
            attempt=row.attempt
            try:payload=self.load_inputs(db,kind,row)
            except Exception as e:
                publish_failure(db,kind,oid,attempt,str(e));return True
        stop=threading.Event()
        heartbeat=threading.Thread(target=self._renew_loop,args=(kind,oid,attempt,stop),daemon=True)
        heartbeat.start()
        try:result=self.solve(kind,payload)
        except Exception as e:
            stop.set();heartbeat.join();traceback.print_exc()
            with self.session_factory() as db:publish_failure(db,kind,oid,attempt,str(e))
            return True
        finally:stop.set()
        heartbeat.join()
        with self.session_factory() as db:published=publish_result(db,kind,oid,attempt,result)
        if published:self.write_artifact(kind,oid,result)
        return True

    def _renew_loop(self,kind,oid,attempt,stop):
        interval=max(1.0,self.lease_seconds/3)
        while not stop.wait(interval):
            try:
                with self.session_factory() as db:
                    if not renew_lease(db,kind,oid,attempt,self.lease_seconds):
                        logging.warning("%s %s attempt %s 租约已丢失，停止续约",kind,oid,attempt);return
            except Exception:logging.exception("租约续约失败: %s %s",kind,oid)

    def write_artifact(self,kind,oid,result):
        """底稿文件是发布成功后的派生副本；规范存储在数据库行内(随状态同事务发布)。"""
        try:
            self.artifacts.mkdir(parents=True,exist_ok=True)
            self.artifacts.joinpath(f"{kind}-{oid}.json").write_text(json.dumps(result,ensure_ascii=False))
        except Exception:logging.exception("底稿文件写入失败: %s %s",kind,oid)

    def reconcile_once(self):
        with self.session_factory() as db:return reconcile(db,self.queue)

    def run_forever(self,poll_timeout=2,reconcile_interval=RECONCILE_INTERVAL_SECONDS):
        self.artifacts.mkdir(parents=True,exist_ok=True)
        try:self.reconcile_once()
        except Exception:logging.exception("启动对账失败")
        threading.Thread(target=self._reconcile_loop,args=(reconcile_interval,),daemon=True).start()
        while True:
            try:
                job=self.queue.blpop(JOB_QUEUE,timeout=poll_timeout)
                if job:self.process_message(job[1])
            except Exception:traceback.print_exc();time.sleep(1)

    def _reconcile_loop(self,interval):
        while True:
            try:
                self.artifacts.joinpath("heartbeat").write_text(str(time.time()))
                stats=self.reconcile_once()
                if stats["lease-expired"] or stats["queued-stale"]:logging.info("对账恢复: %s",stats)
            except Exception:logging.exception("对账循环失败")
            time.sleep(interval)
