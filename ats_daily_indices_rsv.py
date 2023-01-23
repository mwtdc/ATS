#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import urllib.parse
import warnings
import xml.etree.ElementTree as ET
from sys import platform

import pandas as pd
import pymysql
import requests
import yaml
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

start_time = datetime.datetime.now()
warnings.filterwarnings('ignore')

start_date = '2022-01-01'
end_date = '2023-01-18'
# start_date = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
# end_date = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
dates_list = pd.date_range(start=start_date, end=end_date)
# print(dates_list)
list_columns = ['DATE', 'PRICE_ZONE_CODE',
                'CONSUMER_VOLUME', 'CONSUMER_PRICE', 'CONSUMER_RD_VOLUME', 'CONSUMER_SPOT_VOLUME', 'CONSUMER_PROVIDE_RD', 'CONSUMER_MAX_PRICE', 'CONSUMER_MIN_PRICE',
                'SUPPLIER_VOLUME', 'SUPPLIER_PRICE', 'SUPPLIER_RD_VOLUME', 'SUPPLIER_SPOT_VOLUME', 'SUPPLIER_PROVIDE_RD', 'SUPPLIER_MAX_PRICE', 'SUPPLIER_MIN_PRICE',
                'HOUR']

# Настройки для логера

if platform == 'linux' or platform == 'linux2':
    logging.basicConfig(filename='/var/log/log-execute/log_ats_daily_indices_rsv.txt',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s')
elif platform == 'win32':
    logging.basicConfig(filename=f'{pathlib.Path(__file__).parent.absolute()}/log_ats_daily_indices_rsv.txt',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s')

# Загружаем yaml файл с настройками
logging.info('ats_daily_indices_rsv: Старт загрузки файла настроек')
try:
    with open(f'{pathlib.Path(__file__).parent.absolute()}/settings.yaml', 'r') as yaml_file:
        settings = yaml.safe_load(yaml_file)
    telegram_settings = pd.DataFrame(settings['telegram'])
    sql_settings = pd.DataFrame(settings['sql_db'])
    pyodbc_settings = pd.DataFrame(settings['pyodbc_db'])
except Exception as e:
    print(f'ats_daily_indices_rsv: Ошибка загрузки файла настроек: {e}')
    logging.error(f'ats_daily_indices_rsv: Ошибка загрузки файла настроек: {e}')
logging.info('ats_daily_indices_rsv: Финиш загрузки файла настроек')

# Функция отправки уведомлений в telegram на любое количество каналов
#  (указать данные в yaml файле настроек)


def telegram(i, text):
    msg = urllib.parse.quote(str(text))
    bot_token = str(telegram_settings.bot_token[i])
    channel_id = str(telegram_settings.channel_id[i])

    retry_strategy = Retry(
        total=3,
        status_forcelist=[101, 429, 500, 502, 503, 504],
        method_whitelist=["GET", "POST"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    http.post(f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}', timeout=10)


# Функция коннекта к базе Mysql
# (для выбора базы задать порядковый номер числом !!! начинается с 0 !!!!!)


def connection(i):
    host_yaml = str(sql_settings.host[i])
    user_yaml = str(sql_settings.user[i])
    port_yaml = int(sql_settings.port[i])
    password_yaml = str(sql_settings.password[i])
    database_yaml = str(sql_settings.database[i])
    return pymysql.connect(host=host_yaml, user=user_yaml, port=port_yaml,
                           password=password_yaml, database=database_yaml)


try:
    telegram(1, 'ats_daily_indices_rsv: Старт скрапинга суточных индексов РСВ с АТС.')
except Exception as e:
    print(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')
    logging.error(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')

logging.info('ats_daily_indices_rsv: Старт скрапинга суточных индексов РСВ с АТС.')

for z in (1, 2):
    result_dataframe = pd.DataFrame()
    price_zone = z
    print(f'Старт скрапинга по ценовой зоне: {price_zone}')
    logging.info(f'ats_daily_indices_rsv: Старт скрапинга по ценовой зоне: {price_zone}')
    for d in range(len(dates_list)):
        df = pd.DataFrame(columns=list_columns)
        work_date = dates_list[d].strftime('%Y%m%d')
        try:
            url_response = requests.get(f'https://www.atsenergo.ru/market/stats.xml?period=0&date1={work_date}&date2={work_date}&zone={price_zone}&type=graph', verify=False)
        # print(url_response.text)
        except Exception as e:
            print(f'ats_daily_indices_rsv: Ошибка открытия URL: {e}')
            logging.error(f'ats_daily_indices_rsv: Ошибка открытия URL: {e}')
            try:
                telegram(1, f'ats_daily_indices_rsv: Ошибка открытия URL: {e}')
            except Exception as e:
                print(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')
                logging.error(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')
        soup = BeautifulSoup(url_response.text, 'xml')
        rows_list = soup.findAll('row')
        # print(data_list)
        # print(len(data_list))
        for i in range(len(rows_list)):
            root = ET.fromstring(str(rows_list[i]))
            # print(len(root))
            for j in range(len(root)):
                # print(root[j].text)
                df.loc[i, list_columns[j]] = root[j].text
            # print(df)
        result_dataframe = result_dataframe.append(df, ignore_index=True)
    # print(result_dataframe)
    for col in range(2, 15):
        result_dataframe[list_columns[col]] = result_dataframe[list_columns[col]].astype('float')
    for col in (1, 16):
        result_dataframe[list_columns[col]] = result_dataframe[list_columns[col]].astype('int')
    result_dataframe['DATE'] = result_dataframe['DATE'].astype('datetime64[ns]')
    # result_dataframe.to_excel(f'{pathlib.Path(__file__).parent.absolute()}/{start_date}_{end_date}_zone_{price_zone}.xlsx')
    logging.info('ats_daily_indices_rsv: датафрейм сформирован')

    logging.info('ats_daily_indices_rsv: Старт записи индексов хабов в БД.')
    connection_vc = connection(0)
    conn_cursor = connection_vc.cursor()

    vall = ''

    for r in range(len(result_dataframe.index)):
        vall = (vall + "('"
                + str(result_dataframe.DATE[r].strftime("%Y-%m-%d")) + "','"
                + str(result_dataframe.PRICE_ZONE_CODE[r]) + "','"
                + str(result_dataframe.CONSUMER_VOLUME[r]) + "','"
                + str(result_dataframe.CONSUMER_PRICE[r]) + "','"
                + str(result_dataframe.CONSUMER_RD_VOLUME[r]) + "','"
                + str(result_dataframe.CONSUMER_SPOT_VOLUME[r]) + "','"
                + str(result_dataframe.CONSUMER_PROVIDE_RD[r]) + "','"
                + str(result_dataframe.CONSUMER_MAX_PRICE[r]) + "','"
                + str(result_dataframe.CONSUMER_MIN_PRICE[r]) + "','"
                + str(result_dataframe.SUPPLIER_VOLUME[r]) + "','"
                + str(result_dataframe.SUPPLIER_PRICE[r]) + "','"
                + str(result_dataframe.SUPPLIER_RD_VOLUME[r]) + "','"
                + str(result_dataframe.SUPPLIER_SPOT_VOLUME[r]) + "','"
                + str(result_dataframe.SUPPLIER_PROVIDE_RD[r]) + "','"
                + str(result_dataframe.SUPPLIER_MAX_PRICE[r]) + "','"
                + str(result_dataframe.SUPPLIER_MIN_PRICE[r]) + "','"
                + str(result_dataframe.HOUR[r]) + "','"
                + str(datetime.datetime.now().isoformat()) + "'" + '),')

    vall = vall[:-1]
    # print(vall)
    columns_to_db = ', '.join(list_columns)
    try:
        sql = (f'INSERT INTO mydb.daily_indices_rsv ({columns_to_db}, LOAD_TIME) VALUES {vall};')
        conn_cursor.execute(sql)
        connection_vc.commit()
        connection_vc.close()
        logging.info(f'ats_daily_indices_rsv: Финиш записи индексов хабов по ценовой зоне: {price_zone} в БД.')
    except Exception as e:
        print(f'ats_daily_indices_rsv: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        logging.error(f'ats_daily_indices_rsv: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        try:
            telegram(1, f'ats_daily_indices_rsv: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        except Exception as e:
            print(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')
            logging.error(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')
try:
    telegram(1, f'ats_daily_indices_rsv: Финиш скрапинга суточных индексов РСВ за {start_date} с АТС.')
    # telegram(0, f'ats_daily_indices_rsv: Скрапинг суточных индексов РСВ за {start_date} с АТС завершен.')
except Exception as e:
    print(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')
    logging.error(f'ats_daily_indices_rsv: Ошибка отправки в телеграм: {e}')
logging.info('ats_daily_indices_rsv: Финиш скрапинга суточных индексов РСВ с АТС.')

print('Время выполнения:', datetime.datetime.now() - start_time)
