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

    def dumps(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def loads(cls, raw: str) -> "Job":
        return cls(**json.loads(raw))
