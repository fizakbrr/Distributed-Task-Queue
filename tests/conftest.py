import pytest

from taskqueue.broker import Broker


@pytest.fixture
def broker():
    # db=15 keeps test state away from the demo queue in db=0
    b = Broker(db=15)
    b.r.flushdb()
    yield b
    b.r.flushdb()
