import json
import time
import uuid
from dataclasses import asdict, dataclass, field


@dataclass
class Job:
    task: str
    args: list
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    enqueued_at: float = field(default_factory=time.time)
    # Names the business operation, not the delivery attempt. Two jobs with
    # the same key produce the side effect once. Defaults to the job id in
    # __post_init__, which still dedupes redeliveries of this same job.
    idempotency_key: str = ""

    def __post_init__(self):
        if not self.idempotency_key:
            self.idempotency_key = self.id

    def dumps(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def loads(cls, raw: str) -> "Job":
        return cls(**json.loads(raw))
