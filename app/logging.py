import logging

from .config import config, get_logs_dir

def configure_logging():
    logging.basicConfig(
        filename= get_logs_dir(config) / "server.log",
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s - %(threadName)-10s)- %(levelname)s - %(message)s'
    )

