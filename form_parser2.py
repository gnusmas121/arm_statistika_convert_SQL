# form_parser.py (ready to paste)
import os
import re
import logging
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
from config import ConfigLoader
from database import DatabaseConnector
from sqlalchemy import text

class FormParser:
    def __init__(self, config_path="config.yaml"):  
        self.logger = logging.getLogger(__name__)
        cfg = ConfigLoader(config_path)
        self.forms_dir = cfg.forms_directory
        self.db = DatabaseConnector(config_path)
        self.engine = self.db.connect_sqlalchemy()

    def parse_all_forms(self):
        print("=== Обработка всех форм ===")
        for form_folder in os.listdir(self.forms_dir):
            if not re.match(r"\d{4}_\d{2}\.\d+", form_folder):
                continue

            form_number = int(form_folder.split(".")[-1])
            year, month = map(int, form_folder[:7].split("_"))
            start_date = datetime(year, month, 1).date()
            form_path = os.path.join(self.forms_dir, form_folder)

            print(f"Форма {form_number} ({form_folder})")
            print(f" --> {form_path}")

            for fname in os.listdir(form_path):
                if not fname.lower().endswith(".txt"):
                    continue
                fpath = os.path.join(form_path, fname)
                print(f"    Файл: {fname}")
                with open(fpath, encoding='windows-1251') as f:
                    lines = [line.strip() for line in f if line.strip()]

                columns = [line for line in lines if not re.match(r"^\d", line)]
                rows = [(int(line.split()[0]), " ".join(line.split()[1:])) for line in lines if re.match(r"^\d", line)]

                section_number = int(re.findall(r"\.(\d+)", fname)[0])
                print(f"     Раздел: {section_number}, Колонок: {len(columns)}, Строк: {len(rows)}")

                for i, col in enumerate(columns, 1):
                    for row_number, row_name in rows:
                        monthly_dates = self._generate_month_periods(start_date)
                        for m_start, m_end in monthly_dates:
                            stmt = text(f"""
                            MERGE INTO Forms WITH (HOLDLOCK) AS target
                            USING (SELECT {form_number} AS FormNumber, {section_number} AS SectionNumber, {i} AS ColumnNumber,
                                          :ColumnName AS ColumnName, {row_number} AS RowNumber, :RowName AS RowName,
                                          :StarttDate AS StartDate, :EndDate AS EndDate) AS src
                            ON target.FormNumber = src.FormNumber AND target.SectionNumber = src.SectionNumber AND
                               target.ColumnNumber = src.ColumnNumber AND target.RowNumber = src.RowNumber AND
                               target.StartDate = src.StartDate AND target.EndDate = src.EndDate
                            WHEN MATCHED THEN
                                UPDATE SET ColumnName = src.ColumnName, RowName = src.RowName
                            WHEN NOT MATCHED THEN
                                INSERT (FormNumber, SectionNumber, ColumnNumber, ColumnName, RowNumber, RowName, StartDate, EndDate)
                                VALUES (src.FormNumber, src.SectionNumber, src.ColumnNumber, src.ColumnName,
                                        src.RowNumber, src.RowName, src.StartDate, src.EndDate);
                            """)
                            params={
                                "ColumnName": col,
                                "RowName": row_name,
                                "StarttDate": m_start,
                                "EndDate":m_end
                                
                                
                                }
                            with self.engine.begin() as conn:
                                conn.execute(stmt, params)

    def _generate_month_periods(self, start_date):
        print(f"     начало {start_date}")
        end_date = (datetime.today().replace(day=1) - timedelta(days=1)).date()
        print(f"     начало {end_date}")
        current = start_date
        result = []
        while current <= end_date:
            next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = next_month - timedelta(days=1)
            result.append((current, month_end))
            current = next_month
        return result
    def close(self):
       self.db.close()