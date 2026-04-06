import db_work
from config import db_path
from data_loader import DataLoader

db_work.Tables.drop_all(db_path)
db_work.Tables.create_all(db_path)

DataLoader.load_files_to_db()

print(db_work.Queries.marks(db_path), '\n')
print(db_work.Queries.direction_verdict(db_path), '\n')

print(db_work.Queries.good_students(db_path), '\n')
print(db_work.Queries.students_except_direction(db_path, "Дизайн"), '\n')

print(db_work.Queries.good_students_cte(db_path), '\n')
print(db_work.Queries.students_except_direction_cte(db_path, "Дизайн"), '\n')