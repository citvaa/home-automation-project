import logging
import os


def configure_logging(level: str | None = None) -> None:
    """Configure root logger with a sensible default format and level.

    Respects LOG_LEVEL environment variable if present.
    """
    lvl = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, lvl, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Reduce verbosity for noisy third-party libs
    for noisy in ("paho", "influxdb_client", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str):
    return logging.getLogger(name)
