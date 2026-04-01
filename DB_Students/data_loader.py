from pathlib import Path
import db_work
import config

class DataLoader:

    @staticmethod
    def _parse_file(file_path: str|Path, split_symbol: str) -> dict[str, list]:
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

    @staticmethod
    def load_files_to_db(db_name: str|Path = config.db_path, data_path: str|Path = config.simple_data_path) -> None:
        directions_data = DataLoader._parse_file(config.simple_data_path / "directions",',')
        education_levels_data = DataLoader._parse_file(config.simple_data_path / "education_levels",',')
        education_types_data = DataLoader._parse_file(config.simple_data_path / "education_types",',')
        students_data = DataLoader._parse_file(config.simple_data_path / "students",',')

        db_work.data_to_table(directions_data, 'directions', config.db_path)
        db_work.data_to_table(education_levels_data, 'education_levels', config.db_path)
        db_work.data_to_table(education_types_data, 'education_types', config.db_path)
        db_work.data_to_table(students_data, 'students', config.db_path)