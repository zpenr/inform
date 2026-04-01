import db_work
from config import db_path
from data_loader import DataLoader

db_work.Tables.drop_all(db_path)
db_work.Tables.create_all(db_path)

DataLoader.load_files_to_db()

print(db_work.Queries.all_students(db_path))
print(db_work.Queries.students_num_by_directions(db_path))
print(db_work.Queries.students_num_by_edc_types(db_path))
print(db_work.Queries.marks_by_directions(db_path))
print(db_work.Queries.best_students(db_path, "Информационные системы"))
print(db_work.Queries.with_same_name(db_path))
print(db_work.Queries.full_match(db_path))