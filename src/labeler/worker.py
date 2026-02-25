"""Simple worker entrypoint for running background ingestion jobs."""
import asyncio
import logging
from .ingest import run_periodic

LOG = logging.getLogger("labeler.worker")


async def main():
    # Run the periodic ingestion until cancelled. This function is purposely
    # simple so the container can be run as `python -m labeler.worker`.
    stop_event = asyncio.Event()
    try:
        await run_periodic(stop_event=stop_event)
    except asyncio.CancelledError:
        LOG.info("worker cancelled")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("worker interrupted by user")
