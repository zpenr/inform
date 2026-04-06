import sqlite3
from sqlite3 import Connection
from pathlib import Path

class Tables:

    @staticmethod
    def create_all(db_name: str|Path) -> None:
        with sqlite3.connect(db_name) as conn:
            Tables.create_education_types(conn)
            Tables.create_directions(conn)
            Tables.create_educations(conn)
            Tables.create_students(conn)

    @staticmethod
    def drop_all(db_name: str|Path) -> None:
        with sqlite3.connect(db_name) as conn:
            Tables.drop_students(conn)
            Tables.drop_directions(conn)
            Tables.drop_educations(conn)
            Tables.drop_educations_types(conn)


    @staticmethod
    def create_students(conn: Connection) -> None:
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
    def drop_students(conn: Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS students;")

    @staticmethod
    def create_educations(conn: Connection) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS education_levels (
            id_level INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255) NOT NULL
                );"""
        conn.execute(query)

    @staticmethod
    def drop_educations(conn: Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS education_levels;")

    @staticmethod
    def create_directions(conn: Connection) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS directions (
            id_direction INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255) NOT NULL
                );"""
        conn.execute(query)

    @staticmethod
    def drop_directions(conn: Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS directions;")

    @staticmethod
    def create_education_types(conn: Connection) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS education_types (
            id_type INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255) NOT NULL
                );"""
        conn.execute(query)

    @staticmethod
    def drop_educations_types(conn: Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS education_types;")

class Queries:

    @staticmethod
    def all_students(db_name: str|Path) -> tuple:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = "SELECT count(*) FROM students"
            cursor.execute(query)
            return cursor.fetchone()

    @staticmethod
    def students_num_by_directions(db_name: str|Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT d.name as direction_name, COUNT(s.id_student) as student_count
            FROM students s
            JOIN directions d ON s.id_direction = d.id_direction
            GROUP BY d.id_direction, d.name
            ORDER BY student_count DESC"""
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def students_num_by_edc_types(db_name: str|Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT et.name, count(id_student) 
            FROM students as s 
            JOIN education_types as et ON s.id_education_type = et.id_type
            GROUP BY s.id_education_type"""
            cursor.execute(query)
            return cursor.fetchall()
    
    @staticmethod
    def marks_by_directions(db_name: str|Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT max(avg_mark), min(avg_mark), AVG(avg_mark), d.name
            FROM students as s
            JOIN directions as d ON s.id_direction = d.id_direction
            GROUP BY d.name"""
            cursor.execute(query)
            return cursor.fetchall()
    
    @staticmethod
    def marks_complex_stat(db_name: str|Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT d.name, el.name, et.name, AVG(s.avg_mark)
            FROM students as s
            JOIN directions as d ON s.id_direction = d.id_direction
            JOIN education_levels as el ON s.id_level = el.id_level
            JOIN education_types as et ON s.id_education_type = et.id_type
            GROUP BY d.name, el.name, et.name"""
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def best_students(db_name: str|Path, direction: str, limit: int = 5) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT * 
            FROM students as s 
            JOIN directions as d ON d.id_direction = s.id_direction 
            JOIN education_types as et ON s.id_education_type = et.id_type
            WHERE d.name = ? AND et.name = 'Очная'
            ORDER BY s.avg_mark DESC
            LIMIT ?"""
            cursor.execute(query, (direction, limit))
            return cursor.fetchall()

    @staticmethod
    def with_same_name(db_name: str|Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT sum(num) 
            FROM (SELECT id_student, name, surname, count(*) as num 
                FROM students 
                GROUP BY surname
                HAVING num > 1)"""
            cursor.execute(query)
            return cursor.fetchall()
        
    @staticmethod
    def full_match(db_name: str|Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT id_student, name, surname, count(id_student) as num 
            FROM students 
            GROUP BY surname, name, patronymic
            HAVING num > 1
            """
            cursor.execute(query)
            return cursor.fetchall()
    
    @staticmethod
    def marks(db_name: str|Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT
                s.patronymic,
                s.name,
                n.name AS direction,
                s.avg_mark,
                CASE
                WHEN s.avg_mark >= 4.5 THEN 'Отлично'
                WHEN s.avg_mark >= 3.5 THEN 'Хорошо'
                WHEN s.avg_mark >= 2.5 THEN 'Удовлетворительно'
                ELSE 'Неудовлетворительно'
                END AS mark_status
                FROM students AS s
                JOIN directions AS n ON s.id_direction = n.id_direction;"""
            cursor.execute(query)
            return cursor.fetchall()

    @staticmethod
    def direction_verdict(db_name: str|Path) ->list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """SELECT
                s.patronymic,
                s.name,
                n.name AS direction,
                s.avg_mark,
                CASE
                WHEN n.name = 'Информационные системы' THEN 'Крутой чел'
                ELSE 'Ну такой себе тип, сомнительный'
                END AS verdict
                FROM students AS s
                JOIN directions AS n ON s.id_direction = n.id_direction;"""
            cursor.execute(query)
            return cursor.fetchall()
        
    @staticmethod
    def good_students(db_name: str|Path) ->list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            subquery = """SELECT AVG(avg_mark) FROM students"""
            query = f"""SELECT *
                FROM students AS s
                WHERE s.avg_mark >= ({subquery});"""
            cursor.execute(query)
            return cursor.fetchall()
        
    @staticmethod
    def students_except_direction(db_name: str|Path, direction: str) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            subquery = """SELECT d.id_direction FROM directions AS d WHERE d.name = ?"""
            query = f"""SELECT *
                FROM students AS s
                WHERE s.id_direction NOT IN ({subquery});"""
            cursor.execute(query, (direction, ))
            return cursor.fetchall()

    @staticmethod
    def good_students_cte(db_name: str | Path) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """
                WITH avg_score AS (
                    SELECT AVG(avg_mark) AS average FROM students
                )
                SELECT s.*
                FROM students AS s, avg_score
                WHERE s.avg_mark >= avg_score.average;
            """
            cursor.execute(query)
            return cursor.fetchall()


    @staticmethod
    def students_except_direction_cte(db_name: str | Path, direction: str) -> list[tuple]:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            query = """
                WITH target_direction AS (
                    SELECT d.id_direction FROM directions AS d WHERE d.name = ?
                )
                SELECT s.*
                FROM students AS s
                WHERE s.id_direction NOT IN (SELECT id_direction FROM target_direction);
            """
            cursor.execute(query, (direction,))
            return cursor.fetchall()
    
def data_to_table(data: dict[str, list], table_name: str, db_path: str|Path) -> None:
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