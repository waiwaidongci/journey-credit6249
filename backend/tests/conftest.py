import os,sys,tempfile
from pathlib import Path

BACKEND=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BACKEND))
_TMP=tempfile.mkdtemp(prefix="journey-credit-test-")
os.environ["DATABASE_URL"]=f"sqlite:///{_TMP}/test.db"
os.environ.setdefault("REDIS_URL","redis://127.0.0.1:6390/15")

import pytest
from src.config.database import Base,engine,SessionLocal
from src.models import AttributionRun,BudgetScenario,JourneyBatch
from src.utils.serialize import record

CREDITS=[{"code":"A","name":"渠道A","unit_cost":10.0,"capacity":100.0,"association_score":2.0,"share":0.6},
         {"code":"B","name":"渠道B","unit_cost":5.0,"capacity":50.0,"association_score":1.0,"share":0.4}]

class FakeQueue:
    """内存队列：可预置丢消息/重复投递，验证对账与认领守卫。"""
    def __init__(self):self.items=[]
    def rpush(self,key,value):self.items.append(value);return len(self.items)
    def blpop(self,key,timeout=0):return (key,self.items.pop(0)) if self.items else None
    def drain(self):
        while self.items:yield self.blpop("journey:jobs")[1]

class BrokenQueue:
    def rpush(self,*args):raise ConnectionError("redis unavailable")
    def blpop(self,*args,**kwargs):return None

@pytest.fixture()
def db():
    Base.metadata.create_all(engine)
    with SessionLocal() as session:yield session
    Base.metadata.drop_all(engine)

@pytest.fixture()
def queue():return FakeQueue()

@pytest.fixture()
def run(db):
    row=AttributionRun(name="基准研究",batch_id=1,status="completed",
        result={"credits":CREDITS,"input_hash":"attr-hash-1"},owner="测试员")
    db.add(row);db.commit();return row.id

def make_scenario(db,run_id,**fields):
    row=BudgetScenario(name=fields.pop("name","讨论情景"),run_id=run_id,budget=fields.pop("budget",1000.0),
        capacity_overrides=fields.pop("capacity_overrides",{}),owner="测试员",**fields)
    db.add(row);db.commit();return row.id

def fetch(model,oid):
    with SessionLocal() as session:return record(session.get(model,oid))

def audit_actions():
    from src.models import Audit
    with SessionLocal() as session:return [a.action for a in session.query(Audit).order_by(Audit.id)]
