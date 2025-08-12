# data_parser.py (ready to paste)
import os
import re
import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from config import ConfigLoader
from database import DatabaseConnector

class DataParser:
    def __init__(self, config_path="config.yaml"):
       cfg = ConfigLoader(config_path)
       self.data_dir = cfg.data_directory
       db = DatabaseConnector(config_path)
       self.engine = db.connect_sqlalchemy()
       self.folder_pattern = re.compile(r"DAT_(\d{2})(\d{2})\.(\d+)")

    def parse_all_data(self):
        print("=== Загрузка данных ===")
        for entry in os.listdir(self.data_dir):
            full_path = os.path.join(self.data_dir, entry)
            if not os.path.isdir(full_path):
                continue

            match = self.folder_pattern.match(entry)
            if not match:
                print(f"Пропущен каталог: {entry}")
                continue

            year_path, month,form_number = map(int, match.groups())
            year=int(f"20{year_path}")
            print(f" --> {entry} (Форма: {form_number}, Год: {year}, Месяц {month})")

            for fname in os.listdir(full_path):
                file_path = os.path.join(full_path, fname)
                print(f"    Файл: {fname}")
                self._process_data_file(file_path, form_number, year, month, fname)

    def _process_data_file(self, file_path, form_number, year, month, fname):
        try:
            with open(file_path, encoding='windows-1251') as f:
                lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"Ошибка чтения файла {file_path}: {e}")
            return

        if not lines or not lines[1].startswith("!"):
            print(f"Формат файла некорректный: {file_path}")
            return

        header = lines[1].split()
        section = int(header[3])
        dept = int(header[6])
        month = int(header[5])
        print(f" --> Подразделение: {dept}, Месяц: {month}")
        start_date = pd.Timestamp(year=year, month=month, day=1).date()
        end_date = (pd.Timestamp(start_date) + pd.offsets.MonthEnd(1)).date()

        for line in lines[2:]:
            values = list(map(float, line.split()))
            row_number = int(values[0])
            for col_index, val in enumerate(values[1:], 1):
                stmt = text(f"""MERGE INTO Data WITH (HOLDLOCK) AS target
                            USING (SELECT {form_number} AS FormNumber, {col_index} AS ColumnNumber,
                              {row_number} AS RowNumber, {dept} AS DepartmentIndex,
                              {year} AS Year, {month} AS Month, {val} AS Value,
                              '{start_date}' AS StartDate, '{end_date}' AS EndDate,{section} AS SectionNumber) AS src
                ON target.FormNumber = src.FormNumber AND target.SectionNumber = src.SectionNumber AND
                   target.ColumnNumber = src.ColumnNumber AND target.RowNumber = src.RowNumber AND
                   target.DepartmentIndex = src.DepartmentIndex AND target.Year = src.Year AND target.Month = src.Month AND
                   target.StartDate = src.StartDate AND target.EndDate = src.EndDate
                WHEN MATCHED THEN
                    UPDATE SET Value = src.Value
                WHEN NOT MATCHED THEN
                    INSERT (FormNumber, SectionNumber, ColumnNumber, RowNumber, DepartmentIndex,
                            Year, Month, Value, StartDate, EndDate)
                    VALUES (src.FormNumber, src.SectionNumber, src.ColumnNumber, src.RowNumber,
                            src.DepartmentIndex, src.Year, src.Month, src.Value,
                            src.StartDate, src.EndDate);
                """)
                with self.engine.begin() as conn:
                    conn.execute(stmt)

    def close(self):
        self.engine.dispose()