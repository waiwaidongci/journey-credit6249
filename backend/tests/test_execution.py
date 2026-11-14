"""可靠执行验证：落库未入队、队列丢消息、重复投递、执行者崩溃、求解后未发布、
新旧执行者交错完成、获批底稿不可变、完成状态与底稿一致发布。"""
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from src.config.database import SessionLocal
from src.models import AttributionRun,BudgetScenario,Channel,Journey,JourneyBatch
from src.schemas.scenario import ReviewInput,ScenarioInput
from src.services.execution import claim,enqueue,publish_failure,publish_result,reconcile
from src.services.executor import Executor
from src.services.scenario_service import create_scenario,export_scenario,review_scenario

from conftest import BrokenQueue,CREDITS,audit_actions,fetch,make_scenario

ADMIN=SimpleNamespace(name="陈照 · 传播负责人",email="admin@xunguang.demo")
ANALYST=SimpleNamespace(name="许晴 · 路径研究员",email="analyst@xunguang.demo")

def executor(queue,tmp_path,worker_id="w-test",lease=30.0):
    return Executor(queue,SessionLocal,worker_id=worker_id,lease_seconds=lease,artifacts=str(tmp_path))

def test_persisted_but_not_enqueued_recovers(db,run,queue,tmp_path):
    """落库后未入队：创建提交与入队之间崩溃，对账器补投并完成。"""
    sid=make_scenario(db,run)  # 只落库，从未入队（模拟 rpush 前进程崩溃）
    stats=reconcile(db,queue,now_ts=time.time()+60)
    assert stats["queued-stale"]==1 and queue.items==[f"scenario:{sid}"]
    ex=executor(queue,tmp_path)
    for msg in queue.drain():ex.process_message(msg)
    row=fetch(BudgetScenario,sid)
    assert row["status"]=="completed" and row["attempt"]==1
    assert row["result"]["input_hash"] and row["result"]["attribution_hash"]=="attr-hash-1"
    assert (tmp_path/f"scenario-{sid}.json").exists()
    actions=audit_actions()
    assert "scenario.recovered" in actions and "scenario.completed" in actions

def test_queue_message_lost_recovers(db,run,queue,tmp_path,monkeypatch):
    """队列丢消息：入队成功但 Redis 丢失消息，对账器按落库状态重投。"""
    monkeypatch.setattr("src.services.scenario_service.queue",queue)
    rec=create_scenario(db,ScenarioInput(name="丢消息情景",run_id=run,budget=800.0,capacity_overrides={}),ANALYST)
    assert queue.items==[f"scenario:{rec['id']}"]
    queue.items.clear()  # 模拟 Redis 故障丢消息
    stats=reconcile(db,queue,now_ts=time.time()+60)
    assert stats["queued-stale"]==1
    ex=executor(queue,tmp_path)
    for msg in queue.drain():ex.process_message(msg)
    assert fetch(BudgetScenario,rec["id"])["status"]=="completed"

def test_solved_but_not_published_recovers(db,run,queue,tmp_path):
    """求解后未发布：旧执行者认领后崩溃，租约超时由新执行者接管重解并发布。"""
    sid=make_scenario(db,run)
    enqueue(queue,"scenario",sid)
    with SessionLocal() as s:
        crashed=claim(s,"scenario",sid,"old-worker",lease_seconds=0.1)
        assert crashed.attempt==1  # 旧执行者已认领，求解后发布前崩溃
    time.sleep(0.2)
    stats=reconcile(db,queue)
    assert stats["lease-expired"]==1
    ex=executor(queue,tmp_path,worker_id="new-worker")
    for msg in queue.drain():ex.process_message(msg)
    row=fetch(BudgetScenario,sid)
    assert row["status"]=="completed" and row["attempt"]==2 and row["lease_owner"]==""
    assert "scenario.recovered" in audit_actions()

def test_interleaved_executors_stale_result_rejected(db,run,queue):
    """新旧执行者交错完成：超时接管后旧执行者迟到的结果不再生效，获批导出对应同一次求解。"""
    sid=make_scenario(db,run)
    with SessionLocal() as s:
        assert claim(s,"scenario",sid,"old-worker",lease_seconds=0.1).attempt==1
    time.sleep(0.2)  # 旧执行者失联，租约到期
    with SessionLocal() as s:
        assert claim(s,"scenario",sid,"new-worker",lease_seconds=30).attempt==2  # 超时接管
    new_result={"allocations":[{"code":"A","name":"渠道A","allocation":2.0,"unit_cost":10.0,"capacity":100.0,
        "spend":20.0,"association_score":2.0,"proxy_contribution":4.0,"share":0.6}],
        "input_hash":"NEW-HASH","attribution_hash":"attr-hash-1"}
    with SessionLocal() as s:assert publish_result(s,"scenario",sid,2,new_result) is True
    with SessionLocal() as s:assert publish_result(s,"scenario",sid,1,{"allocations":[],"input_hash":"OLD-HASH"}) is False
    with SessionLocal() as s:assert publish_failure(s,"scenario",sid,1,"迟到的失败") is False
    row=fetch(BudgetScenario,sid)
    assert row["status"]=="completed" and row["attempt"]==2
    assert row["result"]["input_hash"]=="NEW-HASH"  # 旧执行者结果未生效
    assert "scenario.fenced" in audit_actions()
    with SessionLocal() as s:  # 复核批准后，页面与导出底稿是同一次完整求解
        review_scenario(s,sid,ReviewInput(status="approved",review_note="批准归档；关联得分非因果ROI。"),ADMIN)
        csv_text=export_scenario(s,sid)
    assert "NEW-HASH" in csv_text and "OLD-HASH" not in csv_text
    assert fetch(BudgetScenario,sid)["result"]["input_hash"] in csv_text

def test_duplicate_delivery_single_solve(db,run,queue,tmp_path):
    """重复投递：认领守卫吸收重复消息，只完整求解一次。"""
    sid=make_scenario(db,run)
    enqueue(queue,"scenario",sid);enqueue(queue,"scenario",sid)
    ex=executor(queue,tmp_path)
    handled=[ex.process_message(msg) for msg in queue.drain()]
    assert handled==[True,False]
    row=fetch(BudgetScenario,sid)
    assert row["status"]=="completed" and row["attempt"]==1

def test_approved_scenario_untouched(db,run,queue):
    """已有获批底稿保持原样：对账不重投，迟到发布被拒绝，导出不变。"""
    sealed={"allocations":[{"code":"A","name":"渠道A","allocation":1.0,"unit_cost":10.0,"capacity":100.0,
        "spend":10.0,"association_score":2.0,"proxy_contribution":2.0,"share":0.6}],"input_hash":"SEALED-HASH"}
    sid=make_scenario(db,run,status="completed",review_status="approved",result=sealed,
        reviewer="陈照 · 传播负责人",review_note="批准研究情景归档。")
    stats=reconcile(db,queue,now_ts=time.time()+3600)
    assert stats=={"lease-expired":0,"queued-stale":0,"errors":0} and not queue.items
    with SessionLocal() as s:assert publish_result(s,"scenario",sid,9,{"input_hash":"TAMPER"}) is False
    row=fetch(BudgetScenario,sid)
    assert row["review_status"]=="approved" and row["result"]["input_hash"]=="SEALED-HASH"
    with SessionLocal() as s:csv_text=export_scenario(s,sid)
    assert "SEALED-HASH" in csv_text and "TAMPER" not in csv_text

def test_create_scenario_queue_outage_recovers(db,run,queue,tmp_path,monkeypatch):
    """入队时队列故障：不再 503 失败，落库 queued 由对账器自动补投完成。"""
    monkeypatch.setattr("src.services.scenario_service.queue",BrokenQueue())
    rec=create_scenario(db,ScenarioInput(name="断队情景",run_id=run,budget=800.0,capacity_overrides={}),ANALYST)
    assert rec["status"]=="queued" and not queue.items
    reconcile(db,queue,now_ts=time.time()+60)
    ex=executor(queue,tmp_path)
    for msg in queue.drain():ex.process_message(msg)
    assert fetch(BudgetScenario,rec["id"])["status"]=="completed"

def test_export_requires_approved_complete_solve(db,run):
    """导出守卫：未获批或底稿未完整发布时不可导出。"""
    sid=make_scenario(db,run,status="completed",review_status="submitted",result={"input_hash":"X","allocations":[]})
    with pytest.raises(HTTPException) as err:export_scenario(db,sid)
    assert err.value.status_code==403
    sid2=make_scenario(db,run,name="残缺底稿",status="completed",review_status="approved",result={})
    with pytest.raises(HTTPException) as err2:export_scenario(db,sid2)
    assert err2.value.status_code==409

def test_run_recovers_after_executor_crash(db,queue,tmp_path):
    """归因研究共用同一套恢复机制：崩溃接管后完成。"""
    batch=JourneyBatch(name="周一批次",source="t",description="d");db.add(batch);db.flush()
    for code,name,cost,cap in [("A","渠道A",10.0,100.0),("B","渠道B",5.0,50.0)]:
        db.add(Channel(code=code,name=name,unit_cost=cost,capacity=cap,description="d"))
    for code,path,conv in [("J-001",["A","B"],True),("J-002",["A"],False),("J-003",["B","A"],True),
                           ("J-004",["A","A","B"],False),("J-005",["B"],True)]:
        db.add(Journey(code=code,batch_id=batch.id,path=path,converted=conv,note=""))
    run_row=AttributionRun(name="崩溃恢复研究",batch_id=batch.id,owner="测试员");db.add(run_row);db.commit()
    enqueue(queue,"run",run_row.id)
    with SessionLocal() as s:assert claim(s,"run",run_row.id,"old-worker",lease_seconds=0.1).attempt==1
    time.sleep(0.2)
    assert reconcile(db,queue)["lease-expired"]==1
    ex=executor(queue,tmp_path,worker_id="new-worker")
    for msg in queue.drain():ex.process_message(msg)
    row=fetch(AttributionRun,run_row.id)
    assert row["status"]=="completed" and row["attempt"]==2
    assert row["result"]["input_hash"] and len(row["result"]["credits"])==2
