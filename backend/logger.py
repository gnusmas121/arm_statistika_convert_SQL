import logging
import os
from config import ConfigLoader

def setup_logger(config_path="config.yaml"):
    """
    Настраивает глобальный логгер: файл + консоль.
    """
    cfg = ConfigLoader(config_path)
    log_file = cfg.log_file
    log_level = getattr(logging, cfg.log_level.upper(), logging.INFO)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
