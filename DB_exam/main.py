import sqlite3
import pandas as pd

def excel_to_sqlite(excel_file, db_name):
    with sqlite3.connect(db_name) as conn:    
        excel_data = pd.read_excel(excel_file, sheet_name=None)
        for sheet_name, df in excel_data.items():
            table_name = sheet_name.replace(' ', '_')
            df.to_sql(table_name, conn, if_exists='replace', index=False)

file_path = '03_2054.xls'
database = 'my_database.db'

excel_to_sqlite(file_path, database)

with sqlite3.connect(database) as conn:
    target_clients_ids = """
    select ID from Clients where Районе='Новый'
"""
    target_services_ids = """
    select ID from Services where Наименование in ('хостинг','видеонаблюдение','установка антивируса')
"""
    services_count_stats = f"""
    select servis_ID, count(servis_ID) as num from Services_provided 
    where client_ID in ({target_clients_ids}) and servis_ID in ({target_services_ids}) 
    group by servis_ID 
"""
    revenue_per_service = f"""
    select * from ({services_count_stats}) as sub join Services ON sub.servis_ID = Services.ID
"""
    total_revenue_query = f"""
    select sum(num*Цена) from ({revenue_per_service})
"""
    result = conn.execute(total_revenue_query)
    print(result.fetchone()[0])