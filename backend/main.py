#main.py

import logging
from logger import setup_logger
from form_parser import FormParser
from data_parser import DataParser

def main():
    setup_logger("config.yaml")
    logger = logging.getLogger(__name__)
    logger.info("=== Запуск парсера ===")

    try:
        form_parser = FormParser("config.yaml")
        form_parser.parse_all_forms()
        form_parser.close()

        data_parser = DataParser("config.yaml")
        data_parser.parse_all_data()
        data_parser.close()

    except Exception as e:
        logger.error(f"Критическая ошибка в main: {e}")

    finally:
        logger.info("=== Завершение работы парсера ===")

if __name__ == "__main__":
    main()
    