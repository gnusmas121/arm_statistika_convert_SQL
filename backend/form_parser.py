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
        self.start_date=cfg.start_date
        self.end_date=cfg.end_date

    def parse_all_forms(self):
        print("=== Обработка всех форм ===")
        '''print (f"стартовые даты {start_date}, {end_date}")
        if start_date is None:
            start_date= datetime.min.date()
        if end_date is None:
            end_date=datetime.max.date()'''
        
        # Группируем каталоги по номеру формы
        form_catalogs = {}
        for entry in os.listdir(self.forms_dir):
            match = re.match(r"(\d{4})_(\d{2})\.(\d+)", entry)
            if not match:
                continue
            year, month, form_number = map(int, match.groups())
            start_date = datetime(year, month, 1).date()
            # Добавляем в список по форме
            if form_number not in form_catalogs:
                form_catalogs[form_number] = []
            form_catalogs[form_number].append((start_date, entry))
        
        # Обрабатываем каждую форму отдельно
        for form_number in form_catalogs:
            catalogs = form_catalogs[form_number]
            catalogs.sort(key=lambda x: x[0])  # Сортируем только папки текущей формы
            
            for i, (start_date, form_folder) in enumerate(catalogs):
                form_path = os.path.join(self.forms_dir, form_folder)
                print(f"Форма {form_number} ({form_folder})")
                print(f" --> {form_path}")

                # Определяем end_date только по папкам текущей формы
                if i + 1 < len(catalogs):
                    next_start_date = catalogs[i + 1][0]
                    end_date = next_start_date - timedelta(days=1)
                else:
                    end_date = (datetime.today().replace(day=1) - timedelta(days=1)).date()
                
                print(f"    Форма {form_number}, Раздел: {form_path}, Start Date: {start_date}, End Date: {end_date}")

                for fname in os.listdir(form_path):
                    fname=fname.lower()
                    if not re.match(r"(\d{3})_(\d{2})(\d{2})\.(\d+)\.txt", fname):
                        continue
                    # Проверка совпадения номера формы в файле и каталоге
                    if int(fname.split('_')[0]) != form_number:
                        print(f"     проверяемая {fname.split('.')[0]}, номер_формы: {form_number}")
                        continue  # Файл другой формы → пропускаем
                    
                    fpath = os.path.join(form_path, fname)
                    print(f"    Файл: {fname}")
                    
                    section_number = int(re.findall(r"\.(\d+)\.txt", fname)[0])
                    print(f"     Раздел: {section_number}")

                    with open(fpath, encoding='windows-1251') as f:
                        lines = [line.strip() for line in f if line.strip()]

                    columns = [line for line in lines if not re.match(r"^\d", line)]
                    rows = [(int(line.split()[0]), " ".join(line.split()[1:])) for line in lines if re.match(r"^\d", line)]

                    print(f"     Колонок: {len(columns)}, Строк: {len(rows)}")

                    for col_idx, col in enumerate(columns, 1):
                        for row_number, row_name in rows:
                            monthly_dates = self._generate_month_periods(start_date, end_date)
                            for m_start, m_end in monthly_dates:
                                stmt = text(f"""
                                MERGE INTO Forms WITH (HOLDLOCK) AS target
                                USING (SELECT {form_number} AS FormNumber, {section_number} AS SectionNumber, {col_idx} AS ColumnNumber,
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
                                params = {
                                    "ColumnName": col,
                                    "RowName": row_name,
                                    "StarttDate": m_start,
                                    "EndDate": m_end
                                }
                                with self.engine.begin() as conn:
                                    conn.execute(stmt, params)

    def _generate_month_periods(self, start_date, end_date):
        print(f"     начало {start_date}")
        print(f"     конец {end_date}")
        current = start_date
        result = []
        while current <= end_date:
            next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = min(next_month - timedelta(days=1), end_date)
            result.append((current, month_end))
            current = next_month
        return result

    def close(self):
        self.db.close()