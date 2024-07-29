import os
from dotenv import load_dotenv
import logging
import yaml
import logging.config
from scripts.connect import DatabaseConnect
from scripts.data_manipulation import DataManipulation

def setup_logging(default_path='config.yaml', default_level=logging.INFO):
    """Чтение конфигурации ведения журнала из файла YAML

            :param default_path: путь к файлу конфигурации журналирования
            :type default_path: str

            :param default_level: уровень логирования
            :type default_level: int
    """
    if os.path.exists(default_path):
        with open(default_path, 'rt') as f:
            log_config = yaml.safe_load(f.read())
        logging.config.dictConfig(log_config)
    else:
        logging.basicConfig(level=default_level)


def main():
    load_dotenv(".env")

    database_name = os.environ.get('DB_NAME')
    user_name = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    port = os.environ.get('DB_PORT')

    connection = DatabaseConnect(database_name, user_name, password, host, port)
    connection.connect_to_db()

    setup_logging()

    table_name = 'dm_f101_round_f'
    data = DataManipulation(table_name, connection)

    data.export_data_from_db()
    data.load_data_to_csv()

    new_table_name = 'dm_f101_round_f_v2'
    data.read_csv_file()

    data.data_file.at[0, 'balance_in_rub'] = 1
    data.data_file.at[1, 'balance_in_rub'] = 1

    data.load_data_to_db(new_table_name)

if __name__ == '__main__':
    main()