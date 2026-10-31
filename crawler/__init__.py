# CRAWLER COORDINATOR
# One shared Frontier is passed to every Worker. Creating separate frontiers
# would defeat cross-thread claims, metadata sharing and request timing.
# THREADCOUNT controls how many workers are actually started; the final run
# selected one, even though this coordinator supports up to four.
from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker


class Crawler:
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        if not 1 <= config.threads_count <= 4:
            raise ValueError("Use 1 to 4 workers; the supplied configuration uses 1")
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = []
        self.worker_factory = worker_factory

    def start_async(self):
        # Construct all workers with the same persistent state/runtime, then
        # start their thread loops. Work ownership is handled inside Frontier.
        self.workers = [
            self.worker_factory(i, self.config, self.frontier)
            for i in range(self.config.threads_count)
        ]
        for worker in self.workers:
            worker.start()

    def start(self):
        try:
            self.start_async()
            self.join()
        except KeyboardInterrupt:
            # Ctrl+C requests graceful shutdown. Requests already inside the
            # blocking downloader must return/time out before workers can exit.
            self.logger.info("Stopping; waiting for active cache calls to return or time out")
            self.frontier.stop()
            # Never close a database while workers might still be writing it.
            while any(worker.is_alive() for worker in self.workers):
                try:
                    self.join()
                except KeyboardInterrupt:
                    self.logger.info("Still waiting for active requests; progress is pending")
        finally:
            # Closing SQLite while an active worker might write would corrupt
            # coordination or raise errors; join first, close the frontier last.
            self.frontier.stop()
            # Also cover failures while starting one of the worker threads.
            for worker in self.workers:
                if worker.ident is not None:
                    worker.join()
            self.frontier.close()
        if self.frontier.runtime.errors:
            raise RuntimeError("Crawl stopped: " + "; ".join(self.frontier.runtime.errors))

    def join(self):
        while any(worker.is_alive() for worker in self.workers):
            for worker in self.workers:
                if worker.ident is not None:
                    worker.join(timeout=0.2)
