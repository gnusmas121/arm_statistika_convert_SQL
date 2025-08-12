#config.py

import yaml
import os

class ConfigLoader:
    """
    Читает конфигурацию из YAML-файла.
    """
    def __init__(self, config_path="config.yaml"):
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        # Database
        db = cfg.get("database", {})
        self.db_server = db.get("server")
        self.db_name   = db.get("name")
        self.db_user   = db.get("username")
        self.db_pass   = db.get("password")
        self.db_encrypt= db.get("encrypt", "no")

        # Paths
        paths = cfg.get("paths", {})
        self.forms_directory = paths.get("forms_directory")  # Изменено
        self.data_directory  = paths.get("data_directory")   # Изменено

        # Logging
        logcfg = cfg.get("logging", {})
        self.log_level = logcfg.get("level", "INFO")
        self.log_file  = logcfg.get("file", "logs/parser.log")
        
        # dates:
        dates = cfg.get("dates", {})
        self.start_date = dates.get("start_date")
        self.end_date = dates.get("end_date")


# Пример использования:
# config = ConfigLoader("config.yaml")
# print(config.forms_dir)
# print(config.data_dir)