"""Dedicated Stream Worker Process Runner.

Decouples Redis Stream consumption and database micro-batch writes from the HTTP web server.
Supports configurable concurrency (number of worker instances), graceful shutdown on SIGTERM/SIGINT,
and container healthcheck heartbeats.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path
import signal
import sys
import uuid

# Ensure src is on sys.path
_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from app.core.config import settings
from app.core.database import async_engine
from app.core.redis import close_redis_pool, get_redis_client
from app.services.stream_worker import StreamWorker

logger = logging.getLogger("stream_worker_manager")

HEALTHCHECK_FILE = Path("/tmp/worker_healthy")


class WorkerManager:
    """Manages multiple StreamWorker consumer instances in a dedicated worker process.

    Provides graceful shutdown on SIGTERM/SIGINT (finishing in-flight batches before exit)
    and maintains container health status via /tmp/worker_healthy.
    """

    def __init__(self, num_workers: int = settings.STREAM_WORKERS_COUNT):
        self.num_workers = max(1, num_workers)
        self.worker_instances: list[StreamWorker] = []
        self.worker_tasks: list[asyncio.Task] = []
        self.is_running = False
        self._heartbeat_task: asyncio.Task | None = None

    def _touch_healthcheck(self) -> None:
        try:
            HEALTHCHECK_FILE.write_text("OK\n")
        except Exception:
            pass

    async def start(self) -> None:
        self.is_running = True
        logger.info(
            f"Starting WorkerManager with {self.num_workers} stream workers "
            f"(target stream: {settings.REDIS_STREAM_WRITE_NAME}, consumer group: {settings.REDIS_STREAM_CONSUMER_GROUP})..."
        )

        # 1. Verify Redis connectivity
        redis = get_redis_client()
        await redis.ping()

        # 2. Initialize consumer group on primary worker
        primary_worker = StreamWorker()
        await primary_worker.init_consumer_group()

        # 3. Instantiate workers
        for i in range(self.num_workers):
            consumer_name = f"worker-{i}-{uuid.uuid4().hex[:6]}"
            worker = StreamWorker(consumer_name=consumer_name)
            self.worker_instances.append(worker)

        # 4. Touch healthcheck file immediately on boot
        self._touch_healthcheck()

        # 5. Create worker tasks
        self.worker_tasks = [
            asyncio.create_task(w.run_loop(), name=w.consumer_name)
            for w in self.worker_instances
        ]

        logger.info(
            f"All {self.num_workers} StreamWorker instances started successfully. "
            f"Decoupled batch ingestion active."
        )

        # 6. Start periodic healthcheck heartbeat loop
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Wait until stopped or a task crashes
        try:
            await asyncio.gather(*self.worker_tasks)
        except asyncio.CancelledError:
            pass
        finally:
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()

    async def _heartbeat_loop(self) -> None:
        """Periodically touches /tmp/worker_healthy for Docker container healthchecks."""
        while self.is_running:
            self._touch_healthcheck()
            await asyncio.sleep(5)

    async def stop(self) -> None:
        """Gracefully stop all stream workers."""
        if not self.is_running:
            return
        logger.info("Stopping all stream workers gracefully...")
        self.is_running = False

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

        for w in self.worker_instances:
            w.stop()

        # Allow in-flight batches up to 10 seconds to commit and XACK
        if self.worker_tasks:
            done, pending = await asyncio.wait(self.worker_tasks, timeout=10.0)
            for t in pending:
                t.cancel()

        try:
            if HEALTHCHECK_FILE.exists():
                HEALTHCHECK_FILE.unlink()
        except Exception:
            pass

        await close_redis_pool()
        await async_engine.dispose()
        logger.info("All stream workers shut down cleanly.")


async def run_manager(num_workers: int) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    manager = WorkerManager(num_workers=num_workers)
    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Received termination signal, initiating graceful worker shutdown...")
        asyncio.create_task(manager.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            # Signal handlers not implemented on Windows event loop
            pass

    try:
        await manager.start()
    except Exception as e:
        logger.exception(f"Unhandled exception in worker manager: {e}")
        await manager.stop()
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dedicated Stream Worker Process")
    parser.add_argument(
        "--workers",
        type=int,
        default=settings.STREAM_WORKERS_COUNT,
        help=f"Number of worker instances (default: {settings.STREAM_WORKERS_COUNT} from .env)",
    )
    args = parser.parse_args()
    asyncio.run(run_manager(num_workers=args.workers))


if __name__ == "__main__":
    main()
