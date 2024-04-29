import logging

from .config import config, get_logs_dir


def configure_logging():
    log_path = get_logs_dir(config) / "server.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_path,
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(threadName)-10s)- %(levelname)s - %(message)s"
    )
