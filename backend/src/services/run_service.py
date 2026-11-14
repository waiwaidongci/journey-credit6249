import logging
from fastapi import HTTPException
from ..models import AttributionRun,JourneyBatch
from ..utils.serialize import record
from .audit_service import log
from .execution import enqueue,queue

def list_runs(db):return [record(r) for r in db.query(AttributionRun).order_by(AttributionRun.id.desc())]
def get_run(db,rid):
    row=db.get(AttributionRun,rid)
    if not row:raise HTTPException(404,"归因研究不存在")
    return row

def create_run(db,data,user):
    if not db.get(JourneyBatch,data.batch_id):raise HTTPException(404,"路径批次不存在")
    row=AttributionRun(**data.model_dump(),owner=user.name)
    db.add(row);db.flush();log(db,user.email,"attribution.enqueue",row.id);db.commit()
    try:enqueue(queue,"run",row.id)
    except Exception:logging.warning("run %s 入队失败，等待对账器补投",row.id,exc_info=True)
    return record(row)
