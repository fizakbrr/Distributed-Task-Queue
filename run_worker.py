from taskqueue.broker import Broker
from taskqueue.worker import Worker
from tasks import TASKS

if __name__ == "__main__":
    Worker(Broker(), TASKS).run()
