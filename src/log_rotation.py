"""
Log rotation — TimedRotatingFileHandler wrapper for daily log files.

Produces: fetcher.log, fetcher.log.2026-02-06, fetcher.log.2026-02-05, ...
Auto-deletes files older than 7 days.
"""

from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def create_rotating_handler(
    log_path: Path,
    backup_count: int = 7,
) -> TimedRotatingFileHandler:
    """Create a daily-rotating file handler with UTC timestamps."""
    handler = TimedRotatingFileHandler(
        filename=log_path,
        when="midnight",
        backupCount=backup_count,
        utc=True,
    )
    return handler
