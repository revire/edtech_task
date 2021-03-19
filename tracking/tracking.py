"""
Используя язык программирования Python автоматизировать выгрузку данных из API системы трекинга мобильных установок.

Входные данные:
токен = 65196fe7aafb
временной диапазон - за последние 60 дней, но со вчерашнего дня
поля для выгрузки - attributed_touch_time, install_time, media_source, campaign, customer_user_id, appsflyer_id
ссылка для выгрузки данных - 'https://hq.appsflyer.com/export/com.igg.android.mobileroyale/installs_report/v5?api_token=&from=&to=&fields='

Сырые данные нужно обогатить полями:
    • Неделя установки
    • Месяц установки
    • Год установки
    • CTIT - в секундах, считается как разница между временем установки и кликом на рекламное размещение
    • CTIT type - группировка по критериям: меньше 10 сек, от 10 до 30 сек, и более 30 сек.

Первично собранный набор данных нужно записать в БД и при повторной реализации скрипта в таблицу должны записываться только новые данные.
Токен не рабочий, поэтому при выполнении запроса будет выдаваться ошибка

Если по этой задаче будут вопросы - https://support.appsflyer.com/hc/ru/categories/201132313-%D0%90%D0%BD%D0%B0%D0%BB%D0%B8%D1%82%D0%B8%D0%BA%D0%B0-%D0%BE%D1%82%D1%87%D0%B5%D1%82%D1%8B-%D0%B8-API
"""


import csv
import logging
import psycopg2
import requests
import datetime

import db_config

logging.basicConfig(level=logging.INFO)

DBNAME= db_config.DBNAME
USER= db_config.USER
PASSWORD= db_config.PASSWORD
HOST= db_config.HOST


def get_data(url):
    logging.info('Collecting dataset from dashboard')
    data = requests.get(url)
    try:
        if data.status_code == 200:

            print(data)
            data_split = list(csv.reader(data.content.decode().splitlines(), delimiter=','))
            agg_report = []
            for row in data_split:
                if row not in agg_report:
                    agg_report.append(row)
            logging.info('The dataset is ready')
            return agg_report
        elif data.status_code != 200:
            logging.warning(f'Status code {data.status_code} while requesting')

    except Exception:
        logging.warning('Exception while requesting URL')




def singleton(cls):
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class DBHandler:
    def __init__(self, dbname: str, user: str, password: str, host: str):
        self.database = dbname
        self.client = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        self.cur = self.client.cursor()
        logging.info(f'Connected to {dbname}')


    def close(self):
        self.client.close()
        self.cur.close()
        logging.info(f'Connection to {self.database} is closed')


    def create_base(self):
        try:
            self.client.set_isolation_level(0)
            self.cur.execute(f'create database {self.database}')
        except psycopg2.errors.DuplicateDatabase:
            logging.warning('Database already exists')


def create_table(con, cur):
    cur.execute('''
        create table if not exists campaign_report (
        attributed_touch_time text,
        install_time text,
        media_source text,
        campaign text,
        customer_user_id text,
        appsflyer_id text
        )
    ''')
    con.commit()
    logging.info('Table campaign_report is created')


def insert_table(con, cur, agg_report):
    for row in agg_report[1:]:
        cur.execute('''
                insert into campaign_report ( 
                    attributed_touch_time,
                    install_time,
                    media_source,
                    campaign,
                    customer_user_id,
                    appsflyer_id
                ) values (
                    nullif(%s, ''),
                    nullif(%s, ''),
                    nullif(%s, ''),
                    nullif(%s, ''),
                    nullif(%s, ''),
                    nullif(%s, '')
                )
                on conflict do nothing 
            ''', row)
    con.commit()
    logging.info('Data is inserted')


if __name__ == '__main__':

    token = '65196fe7aafb'
    fields = ','.join([
        'attributed_touch_time',
        'install_time',
        'media_source',
        'campaign',
        'customer_user_id',
        'appsflyer_id',
    ])

    from_time = datetime.date.today() - datetime.timedelta(days=1)
    to_time = from_time - datetime.timedelta(days=60)

    url = f'https://hq.appsflyer.com/export/com.igg.android.mobileroyale/installs_report/v5?api_token={token}&from={from_time}&to={to_time}&fields={fields}'

    # agg_report = get_data(url)
    with open('sample_report.csv', 'r') as f:
        agg_report = f.readlines()
        agg_report = [line.strip().split(',') for line in agg_report]
        print(agg_report)
        print('got agg report by csv')

    db_connection = DBHandler(DBNAME, USER, PASSWORD, HOST)

    db_connection.create_base()
    con = db_connection.client
    cur = db_connection.cur
    create_table(con, cur)
    insert_table(con, cur, agg_report)


