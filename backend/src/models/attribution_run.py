from sqlalchemy import ForeignKey,JSON
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now
class AttributionRun(Base):
    __tablename__="attribution_runs"
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]
    batch_id:Mapped[int]=mapped_column(ForeignKey("journey_batches.id"))
    status:Mapped[str]=mapped_column(default="queued")
    result:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str]=mapped_column(default="")
    owner:Mapped[str]
    attempt:Mapped[int]=mapped_column(default=0)
    lease_owner:Mapped[str]=mapped_column(default="")
    lease_expires_at:Mapped[float]=mapped_column(default=0.0)
    created_at:Mapped[str]=mapped_column(default=now)
    updated_at:Mapped[str]=mapped_column(default=now)
