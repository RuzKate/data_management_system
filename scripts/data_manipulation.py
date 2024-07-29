import logging
import pandas as pd
from sqlalchemy.sql import text
from scripts.connect import DatabaseConnect

class DataManipulation:
    def __init__(self, table_name, connection: DatabaseConnect):
        """Инициализатор класса

            :param table_name: название таблицы
            :type table_name: str
            :param connection: движок, отвечающий за взаимодействие с базой данных
            :type connection: DatabaseConnect
        """
        self.table_name = table_name
        self.path_file = f'{table_name}.csv'
        self.engine = connection.engine
        self.data_db = None
        self.data_file = None


    def export_data_from_db(self):
        """Экспорт данных из базы данных"""
        try:
            self.data_db = pd.read_sql_table(self.table_name, self.engine, schema='dm')
            logging.info(f"Reading table from database '{self.table_name}' in DataFrame")
        except Exception as error:
            logging.error(f"Error reading table from database '{self.table_name}' in DataFrame", exc_info=True)
            exit()
    

    def load_data_to_csv(self): 
        """Загрузка данных в csv-файл"""          
        try:
            self.data_db.to_csv(self.path_file, index=False)
            logging.info(f"Writing a DataFrame to a csv-file '{self.path_file}'")
        except Exception as error:
            logging.error(f"Error writing a DataFrame to a csv-file '{self.path_file}'", exc_info=True)
            exit()


    def read_csv_file(self):
        """Чтение данных из csv-файла"""
        try:
            self.data_file = pd.read_csv(self.path_file)
            logging.info(f"Reading table from csv-file '{self.path_file}' in DataFrame")
        except Exception as error:
            logging.error(f"Error reading table from csv-file '{self.path_file}' in DataFrame", exc_info=True)
            exit()


    def load_data_to_db(self, new_table_name):
        """Загрузка данных в базу данных

            :param new_table_name: название таблицы
            :type new_table_name: str
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(text(f'truncate dm.{new_table_name};'))
                connection.commit()

                logging.info(f"TRUNCATE dm.{new_table_name}")
        except Exception as error:
            logging.error(f"Error TRUNCATE dm.{new_table_name}", exc_info=True)
            exit()

        try:
            self.data_file.to_sql(f'{new_table_name}', con=self.engine, if_exists='append', index=False, schema='dm')
            logging.info(f"Loading a DataFrame into a table '{new_table_name}' in the database")
        except Exception as error:
            logging.error(f"Error loading a DataFrame into a table '{new_table_name}' in the database", exc_info=True)
            exit()