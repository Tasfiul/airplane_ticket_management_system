"""Standalone background worker to consume write events from Redis Stream and

commit transactions to PostgreSQL via PgBouncer.
"""

import asyncio
import logging
from app.services.stream_worker import stream_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main():
    print("Starting Redis Stream DB Write Worker...")
    try:
        await stream_worker.run_loop()
    except KeyboardInterrupt:
        print("Stopping worker...")
        stream_worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
