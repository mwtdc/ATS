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

# start_date = '2022-01-01'
# end_date = '2022-12-31'
last_day_of_previous_month = datetime.date.today().replace(day=1) + datetime.timedelta(days=-1)
first_day_of_previous_month = datetime.date(day=1,
                                            month=last_day_of_previous_month.month,
                                            year=last_day_of_previous_month.year)
start_date = (first_day_of_previous_month).strftime("%Y-%m-%d")
end_date = (last_day_of_previous_month).strftime("%Y-%m-%d")
dates_list = pd.date_range(start=start_date, end=end_date)
print(dates_list)
list_columns = ['DATE', 'HOUR',
                'K_PLAN', 'K_FACT']

# Настройки для логера

if platform == 'linux' or platform == 'linux2':
    logging.basicConfig(filename='/var/log/log-execute/log_ats_carbon_dioxide_emission.txt',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s')
elif platform == 'win32':
    logging.basicConfig(filename=f'{pathlib.Path(__file__).parent.absolute()}/log_ats_carbon_dioxide_emission.txt',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s')

# Загружаем yaml файл с настройками
logging.info('ats_carbon_dioxide_emission: Старт загрузки файла настроек')
try:
    with open(f'{pathlib.Path(__file__).parent.absolute()}/settings.yaml', 'r') as yaml_file:
        settings = yaml.safe_load(yaml_file)
    telegram_settings = pd.DataFrame(settings['telegram'])
    sql_settings = pd.DataFrame(settings['sql_db'])
    pyodbc_settings = pd.DataFrame(settings['pyodbc_db'])
except Exception as e:
    print(f'ats_carbon_dioxide_emission: Ошибка загрузки файла настроек: {e}')
    logging.error(f'ats_carbon_dioxide_emission: Ошибка загрузки файла настроек: {e}')
logging.info('ats_carbon_dioxide_emission: Финиш загрузки файла настроек')

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
    telegram(1, 'ats_carbon_dioxide_emission: Старт скрапинга коэффициента выбросов CO2 с АТС.')
except Exception as e:
    print(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')
    logging.error(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')

logging.info('ats_carbon_dioxide_emission: Старт скрапинга коэффициента выбросов CO2 с АТС.')

for z in (1,):
    result_dataframe = pd.DataFrame()
    price_zone = z
    print(f'Старт скрапинга по ценовой зоне: {price_zone}')
    logging.info(f'ats_carbon_dioxide_emission: Старт скрапинга по ценовой зоне: {price_zone}')
    for d in range(len(dates_list)):
        df = pd.DataFrame(columns=list_columns)
        work_date = dates_list[d].strftime('%Y%m%d')
        try:
            url_response = requests.get(f'https://www.atsenergo.ru/market/stats.xml?type=carbon&date1={work_date}&date2={work_date}&zone={price_zone - 1}&period=0', verify=False)
        # print(url_response.text)
        except Exception as e:
            print(f'ats_carbon_dioxide_emission: Ошибка открытия URL: {e}')
            logging.error(f'ats_carbon_dioxide_emission: Ошибка открытия URL: {e}')
            try:
                telegram(1, f'ats_carbon_dioxide_emission: Ошибка открытия URL: {e}')
            except Exception as e:
                print(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')
                logging.error(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')
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
    for col in (1, 3):
        result_dataframe[list_columns[col]] = result_dataframe[list_columns[col]].astype('int')
    result_dataframe['DATE'] = result_dataframe['DATE'].astype('datetime64[ns]')
    result_dataframe['SZ_CODE'] = price_zone
    result_dataframe['SZ_CODE'] = result_dataframe['SZ_CODE'].astype('int')
    # print(result_dataframe)
    # result_dataframe.to_excel(f'{pathlib.Path(__file__).parent.absolute()}/{start_date}_{end_date}_zone_{price_zone}.xlsx')
    logging.info('ats_carbon_dioxide_emission: датафрейм сформирован')

    logging.info('ats_carbon_dioxide_emission: Старт записи коэффициента выбросов CO2 в БД.')
    connection_vc = connection(0)
    conn_cursor = connection_vc.cursor()

    vall = ''

    for r in range(len(result_dataframe.index)):
        vall = (vall + "('"
                + str(result_dataframe.DATE[r].strftime("%Y-%m-%d")) + "','"
                + str(result_dataframe.HOUR[r]) + "','"
                + str(result_dataframe.K_PLAN[r]) + "','"
                + str(result_dataframe.K_FACT[r]) + "','"
                + str(result_dataframe.SZ_CODE[r]) + "','"
                + str(datetime.datetime.now().isoformat()) + "'" + '),')

    vall = vall[:-1]
    # print(vall)
    columns_to_db = ', '.join(list_columns)
    try:
        sql = (f'INSERT INTO treid_03.carbon_dioxide_emission ({columns_to_db}, SZ_CODE, LOAD_TIME) VALUES {vall};')
        conn_cursor.execute(sql)
        connection_vc.commit()
        connection_vc.close()
        logging.info(f'ats_carbon_dioxide_emission: Финиш записи коэффициента выбросов CO2 по ценовой зоне: {price_zone} в БД.')
    except Exception as e:
        print(f'ats_carbon_dioxide_emission: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        logging.error(f'ats_carbon_dioxide_emission: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        try:
            telegram(1, f'ats_carbon_dioxide_emission: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        except Exception as e:
            print(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')
            logging.error(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')
try:
    telegram(1, f'ats_carbon_dioxide_emission: Финиш скрапинга коэффициента выбросов CO2 за {start_date} с АТС.')
    telegram(0, f'ats_carbon_dioxide_emission: Скрапинг коэффициента выбросов CO2 за предыдущий месяц с АТС завершен.')
except Exception as e:
    print(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')
    logging.error(f'ats_carbon_dioxide_emission: Ошибка отправки в телеграм: {e}')
logging.info('ats_carbon_dioxide_emission: Финиш скрапинга коэффициента выбросов CO2 с АТС.')

print('Время выполнения:', datetime.datetime.now() - start_time)
