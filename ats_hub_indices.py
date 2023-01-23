#!/usr/bin/python3.9
#!/usr/bin/env python
# coding: utf-8

import datetime
import logging
import pathlib
import urllib.parse
import warnings
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

# Настройки для логера

if platform == 'linux' or platform == 'linux2':
    logging.basicConfig(filename='/var/log/log-execute/log_ats_hub_indices.txt',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s')
elif platform == 'win32':
    logging.basicConfig(filename=f'{pathlib.Path(__file__).parent.absolute()}/log_ats_hub_indices.txt',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s')

# Загружаем yaml файл с настройками
logging.info('ats_hub_indices: Старт загрузки файла настроек')
try:
    with open(f'{pathlib.Path(__file__).parent.absolute()}/settings.yaml', 'r') as yaml_file:
        settings = yaml.safe_load(yaml_file)
    telegram_settings = pd.DataFrame(settings['telegram'])
    sql_settings = pd.DataFrame(settings['sql_db'])
    pyodbc_settings = pd.DataFrame(settings['pyodbc_db'])
except Exception as e:
    print(f'ats_hub_indices: Ошибка загрузки файла настроек: {e}')
    logging.error(f'ats_hub_indices: Ошибка загрузки файла настроек: {e}')
logging.info('ats_hub_indices: Финиш загрузки файла настроек')

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
    telegram(1, 'ats_hub_indices: Старт скрапинга индексов хаба с АТС.')
except Exception as e:
    print(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')
    logging.error(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')

logging.info('ats_hub_indices: Старт скрапинга индексов хаба с АТС.')

for z in (1, 2):
    day_dataframe = pd.DataFrame()
    price_zone = z
    print(f'Старт скрапинга по ценовой зоне: {price_zone}')
    logging.info(f'ats_hub_indices: Старт скрапинга по ценовой зоне: {price_zone}')
    for d in range(len(dates_list)):
        try:
            url_response = requests.get(f'https://www.atsenergo.ru/market/stats.xml?type=hubs&date={dates_list[d].strftime("%Y%m%d")}&zone={price_zone}', verify=False)
        except Exception as e:
            print(f'ats_hub_indices: Ошибка открытия URL: {e}')
            logging.error(f'ats_hub_indices: Ошибка открытия URL: {e}')
            try:
                telegram(1, f'ats_hub_indices: Ошибка открытия URL: {e}')
            except Exception as e:
                print(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')
                logging.error(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')
        soup = BeautifulSoup(url_response.text, "xml")
        data_list = soup.findAll('data')
        df = pd.DataFrame(columns=['date', 'code', 'hour', 'hubdev', 'hubindex', 'zoneindex'])

        for i in range(len(data_list)):
            df.loc[i, 'date'] = dates_list[d].strftime("%d.%m.%Y")
            df.loc[i, 'code'] = data_list[i]['code']
            df.loc[i, 'hour'] = data_list[i]['hour']
            df.loc[i, 'hubdev'] = data_list[i]['hubdev']
            df.loc[i, 'hubindex'] = data_list[i]['hubindex']
            df.loc[i, 'zoneindex'] = data_list[i]['zoneindex']
        day_dataframe = day_dataframe.append(df, ignore_index=True)
        # print(day_dataframe)
    # print(day_dataframe)
    day_dataframe['date'] = day_dataframe['date'].astype('datetime64[ns]')
    day_dataframe['code'] = day_dataframe['code'].astype('str')
    day_dataframe['hour'] = day_dataframe['hour'].astype('int')
    day_dataframe['hubdev'] = day_dataframe['hubdev'].astype('float')
    day_dataframe['hubindex'] = day_dataframe['hubindex'].astype('float')
    day_dataframe['zoneindex'] = day_dataframe['zoneindex'].astype('float')
    # day_dataframe.to_excel(f'{pathlib.Path(__file__).parent.absolute()}/{start_date}_{end_date}_zone_{price_zone}.xlsx')
    logging.info('ats_hub_indices: датафрейм сформирован')

    logging.info('ats_hub_indices: Старт записи индексов хабов в БД.')
    connection_vc = connection(0)
    conn_cursor = connection_vc.cursor()

    vall = ''

    for r in range(len(day_dataframe.index)):
        vall = (vall + "('"
                + str(day_dataframe.date[r].strftime("%Y-%m-%d")) + "','"
                + str(day_dataframe.code[r]) + "','"
                + str(day_dataframe.hour[r]) + "','"
                + str(day_dataframe.hubdev[r]) + "','"
                + str(day_dataframe.hubindex[r]) + "','"
                + str(day_dataframe.zoneindex[r]) + "','"
                + str(datetime.datetime.now().isoformat()) + "'" + '),')

    vall = vall[:-1]
    # print(vall)
    try:
        sql = (f'INSERT INTO mydb.hub_indices (date, code, hour, hubdev, hubindex, zoneindex, load_time) VALUES {vall};')
        conn_cursor.execute(sql)
        connection_vc.commit()
        connection_vc.close()
        logging.info(f'ats_hub_indices: Финиш записи индексов хабов по ценовой зоне: {price_zone} в БД.')
    except Exception as e:
        print(f'ats_hub_indices: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        logging.error(f'ats_hub_indices: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        try:
            telegram(1, f'ats_hub_indices: Ошибка записи значений по ценовой зоне: {price_zone} в БД: {e}')
        except Exception as e:
            print(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')
            logging.error(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')
try:
    telegram(1, f'ats_hub_indices: Финиш скрапинга индексов хаба за {start_date} с АТС.')
    # telegram(0, f'ats_hub_indices: Скрапинг индексов хаба за {start_date} с АТС завершен.')
except Exception as e:
    print(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')
    logging.error(f'ats_hub_indices: Ошибка отправки в телеграм: {e}')
logging.info('ats_hub_indices: Финиш скрапинга индексов хаба с АТС.')

print('Время выполнения:', datetime.datetime.now() - start_time)
