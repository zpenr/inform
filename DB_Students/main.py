import sqlite3
from pathlib import Path

class Tables:

    @staticmethod
    def create_all(db_name):
        Tables.create_education_types(db_name)
        Tables.create_directions(db_name)
        Tables.create_educations(db_name)
        Tables.create_students(db_name)

    @staticmethod
    def create_students(db_name):
        with sqlite3.connect(db_name) as conn:
            query = """
            CREATE TABLE IF NOT EXISTS students (
                id_student INTEGER PRIMARY KEY AUTOINCREMENT,
                id_level INTEGER NOT NULL,
                id_direction INTEGER NOT NULL,
                id_education_type INTEGER NOT NULL,
                surname VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                patronymic VARCHAR(255),
                avg_mark DECIMAL(3,2),
                FOREIGN KEY (id_level) REFERENCES education_levels(id_level),
                FOREIGN KEY (id_direction) REFERENCES directions(id_direction),
                FOREIGN KEY (id_education_type) REFERENCES education_types(id_type));"""
            conn.execute(query)

    @staticmethod
    def create_educations(db_name):
        with sqlite3.connect(db_name) as conn:
            query = """
            CREATE TABLE IF NOT EXISTS education_levels (
                id_level INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL
                    );"""
            conn.execute(query)

    @staticmethod
    def create_directions(db_name):
        with sqlite3.connect(db_name) as conn:
            query = """
            CREATE TABLE IF NOT EXISTS directions (
                id_direction INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL
                    );"""
            conn.execute(query)

    @staticmethod
    def create_education_types(db_name):
        with sqlite3.connect(db_name) as conn:
            query = """
            CREATE TABLE IF NOT EXISTS education_types (
                id_type INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL
                    );"""
            conn.execute(query)

def parse_file(file_path, split_symbol):
    with open(file_path) as file:
        names = file.readline().split(split_symbol)
        data = {}
        
        for name in names:
            data[name] = []

        for line in file.readlines():
            values = line.split(split_symbol)
            for i, value in enumerate(values):
                data[names[i]].append(value.strip())

    return data

def paste_data(data: dict[str, list], table_name, db_path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        fields = list(data.keys())
        placeholders = ', '.join(['?' for _ in fields])
        query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
        
        num_records = len(data[fields[0]])
        
        for i in range(num_records):
            row = [data[field][i] for field in fields]
            cursor.execute(query, row)        
        conn.commit()

base_path = Path(__file__).parent
db_path = base_path / "mydb.db"
simple_data_path = base_path / "simple_data"


Tables.create_all(db_path)

directions_data = parse_file(simple_data_path / "directions",',')
education_levels_data = parse_file(simple_data_path / "education_levels",',')
education_types_data = parse_file(simple_data_path / "education_types",',')
students_data = parse_file(simple_data_path / "students",',')

paste_data(directions_data, 'directions', db_path)
paste_data(education_levels_data, 'education_levels', db_path)
paste_data(education_types_data, 'education_types', db_path)
paste_data(students_data, 'students', db_path)

class Queries:

    @staticmethod
    def all_students(db_name):
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = "SELECT count(id_student) FROM students"
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def students_by_directions(db_name):
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT d.name as direction_name, COUNT(s.id_student) as student_count
            FROM students s
            JOIN directions d ON s.id_direction = d.id_direction
            GROUP BY d.id_direction, d.name
            ORDER BY student_count DESC"""
            cursor.execute(query)
            return cursor.fetchall()

print(Queries.all_students(db_path))
print(Queries.students_by_directions(db_path))