import mysql.connector
from sqlalchemy import create_engine
import pandas as pd




class Database():
    def __init__(self, table, db_config):
        self.db_config = db_config
        self.connection = mysql.connector.connect(**db_config)
        self.cursor = self.connection.cursor(dictionary=True)
        self.table = table

    @staticmethod
    def escape_sql_string(value):
        trans_table = {ord(','): None, ord(':'): None, ord('.'): None, ord('&'): None, ord('!'): None, ord('"'): None,
                       ord('?'): None, ord('\n'): None, ord('\t'): None, ord('@'): None, ord("'"): None, ord("’"): None,
                       ord("Ö"): None}
        return value.translate(trans_table)

    def insert(self,table_name, data: dict):
        keys = ", ".join(data.keys())
        values = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table_name} ({keys}) VALUES ({values})"
        self.cursor.execute(query, tuple(data.values()))
        self.connection.commit()

    def select_all(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name};")
        res = self.cursor.fetchall()
        return pd.DataFrame(res)

    def create(self, table_name: str, columns: dict):
        # формируем список колонок
        column_definitions = ", ".join(
            [f"{name} {datatype}" for name, datatype in columns.items()]
        )

        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {column_definitions}
            );
        """

        self.cursor.execute(query)
        self.connection.commit()


    def update(self, table_name, id, column, new_text):
        self.cursor.execute(f"""
        UPDATE `{table_name}` SET `{column}` = '{new_text}' WHERE id = {id};  
        """)
        self.connection.commit()

    def select(self, table_name, colomn, reverse: bool):

        if reverse == True:
            self.cursor.execute(f"""
            SELECT {colomn} FROM {table_name} ORDER BY id DESC;
        """)
        else:
            self.cursor.execute(f"""
                        SELECT {colomn} FROM {table_name};
                    """)

        res = self.cursor.fetchall()
        return pd.DataFrame(res)


    def print_for_id(self, table_name, firs_id, last_id):
        self.cursor.execute(f"SELECT * FROM {table_name} WHERE id")
        for i in range(firs_id, last_id):
            items = self.cursor.fetchmany(i)
            print(items)


    def delete_for_id(self, table_name, firt_id, last_id):
        for i in range(firt_id, last_id+1):
            self.cursor.execute(f"DELETE FROM {table_name} WHERE id = {i};")
            self.connection.commit()


    def describe(self, table_name):
        self.cursor.execute(f"DESCRIBE {table_name};")
        res = self.cursor.fetchall()
        return pd.DataFrame(res)

    def stroka_stolbec(self, table_name, name, stolbec_name):
        self.cursor.execute(f"SELECT * FROM {table_name} WHERE {stolbec_name} = '{name}'")
        res = self.cursor.fetchall()
        return pd.DataFrame(res)


    def delete_table(self, table_name):
        self.cursor.execute(f"DROP TABLE {table_name};")
        self.connection.commit()


    def delete_column(self,table_name, column):
        self.cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column};")
        self.connection.commit()


    def add_column(self,table_name, column, type):
        self.cursor.execute(f"ALTER TABLE {table_name} ADD {column} {type};")
        self.connection.commit()


    def export_csv(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name};")
        res = self.cursor.fetchall()
        df = pd.DataFrame(res)
        df.to_csv(f"{table_name}_file.csv", index = False)



    def improt_csv(self,table_name, file_name):
        df = pd.read_csv(file_name)
        df.to_sql(table_name, con=engine, if_exists='append', index=False)


    def foreign_key(self, table_from, column, table_to):
        self.cursor.execute(f"ALTER TABLE `{table_from}` ADD FOREIGN KEY (`{column}`) "
                            f"REFERENCES `{table_to}`(`id`) ON DELETE RESTRICT ON UPDATE RESTRICT;")
        self.connection.commit()


tab = Database("zolotova_fitnes_clubs", db_config)


tab.foreign_key("zolotova_client_info", "client_id", "zolotova_fitnes_club_clients")
# columns = {
#     "client_id" : "INT",
#     "name" : "TEXT",
#     "surname" : "TEXT",
#     "age" : "INT"
# }
# tab.create("zolotova_client_info", columns)
#tab.update(table_name="zolotova_fitnes_clubs", id="1", column="times_open", new_text="9:00-23:00")
tab.insert("zolotova_client_info",
            {'client_id': 3,
            'name': "Екатерина",
            'surname' : "Якушева",
            'age' : 17
            })
#print(tab.select("zolotova_fitnes_club_clients",  "name", True))
#tab.print_for_id("zolotova_fitnes_clubs", 0, 3)
#tab.delete_for_id("zolotova_test", 2, 3)
#print(tab.describe("zolotova_fitnes_clubs"))
#print(tab.stroka_stolbec("zolotova_fitnes_club_coach", "Петр", "name"))
#tab.delete_table("zolotova_test")
# tab.add_column("zolotova_test", "zxc", "VARCHAR(100)")
#tab.delete_column("zolotova_fitnes_club_clients", "age")
# tab.export_csv("zolotova_fitnes_clubs")
# tab.improt_csv("zolotova_import", "zolotova_fitnes_clubs_file.csv")