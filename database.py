import pyodbc
import logging
from sqlalchemy import create_engine
from config import ConfigLoader

class DatabaseConnector:
    """
    Создаёт pyodbc.Connection и SQLAlchemy Engine на основе config.yaml.
    """
    def __init__(self, config_path="config.yaml"):
        self.logger = logging.getLogger(__name__)
        cfg = ConfigLoader(config_path)

        # Собираем строку ODBC для pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={cfg.db_server};"
            f"DATABASE={cfg.db_name};"
            f"UID={cfg.db_user};"
            f"PWD={cfg.db_pass};"
            f"Encrypt={cfg.db_encrypt};"
        )
        try:
            self.pyodbc_conn = pyodbc.connect(conn_str)
            self.logger.info("pyodbc: Успешное подключение к базе данных.")
        except pyodbc.Error as e:
            self.logger.error(f"pyodbc: Ошибка подключения: {e}")
            raise

        # Собираем строку для SQLAlchemy Engine
        # Пример: mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no
        driver_str = "ODBC+Driver+18+for+SQL+Server"
        encrypt_flag = cfg.db_encrypt
        engine_url = (
            f"mssql+pyodbc://{cfg.db_user}:{cfg.db_pass}"
            f"@{cfg.db_server}/{cfg.db_name}"
            f"?driver={driver_str}&encrypt={encrypt_flag}"
        )
        try:
            self.engine = create_engine(engine_url)
            self.logger.info("SQLAlchemy: Успешно создан Engine.")
        except Exception as e:
            self.logger.error(f"SQLAlchemy: Ошибка создания Engine: {e}")
            raise

    def connect_sqlalchemy(self):
        """
        Возвращает SQLAlchemy Engine.
        """
        return self.engine

    def close(self):
        """
        Закрывает pyodbc-соединение.
        """
        try:
            self.pyodbc_conn.close()
            self.logger.info("pyodbc: Соединение закрыто.")
        except Exception:
            pass
