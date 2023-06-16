from time import sleep

import pandas as pd
import csv
import mysql.connector
import wget
import os
import xml.etree.ElementTree as ET
import requests
# Курс валют
def get_current():
    url = "https://api.privatbank.ua/p24api/pubinfo?exchange&coursid=5"
    response = requests.get(url)
    data = response.json()
    usd_sale = None
    for currency in data:
        if currency["ccy"] == "USD":
            usd_sale = currency["sale"]
            break
    return float(usd_sale)

def download_price_sun():
    sun_url = 'http://suncomp.com.ua/sun_notebook_parts.xls'

    file_path = '/home/vasyl/PycharmProjects/Updating-prices-in-the-database/sun_notebook_parts.xls'

    if os.path.exists(file_path):
        os.remove(file_path)
        print(f'Файл {file_path} удален.')
    else:
        print(f'Файл {file_path} не найден.')

    wget.download(sun_url, '/home/vasyl/PycharmProjects/Updating-prices-in-the-database/sun_notebook_parts.xls')


def download_price_4l():
    l4_url = 'https://4laptop.kiev.ua/price/google.xml'

    file_path = '/home/vasyl/PycharmProjects/Updating-prices-in-the-database/google.xml'

    if os.path.exists(file_path):
        os.remove(file_path)
        print(f'Файл {file_path} удален.')
    else:
        print(f'Файл {file_path} не найден.')

    wget.download(l4_url, '/home/vasyl/PycharmProjects/Updating-prices-in-the-database/google.xml')

def download_price_ak():
    session = requests.Session()
    login_data = {
        'login[mail]': 'a0968598177@gmail.com',
        'login[password]': 'smart555'
    }
    session.post('https://a-class.com.ua/uk/login', data=login_data)
    response = session.get('https://a-class.com.ua/uk/account/a-class_price.xlsx')

    file_path2 = '/home/vasyl/PycharmProjects/Updating-prices-in-the-database/a-class_price.xlsx'
    if os.path.exists(file_path2):
        os.remove(file_path2)
        print(f'Файл {file_path2} удален.')
    else:
        print(f'Файл {file_path2} не найден.')
    with open('a-class_price.xlsx', 'wb') as file:
        file.write(response.content)


connection = mysql.connector.connect(
        host='localhost',
        port=3306,
        database='IDs',
        user='root2',
        password='qwerty'
    )

# Создаем пустой DataFrame для хранения результирующих данных
# price_usd = pd.DataFrame(columns=['ID', 'Key_zeto', 'Price_sun', 'Price_dfi', 'Price_arc', 'Price_ak', 'Price_4l', 'Price_pp', 'Price_dc'])
price_usd = pd.DataFrame(columns=['ID', 'Price_sun', 'Price_dfi', 'Price_arc', 'Price_ak', 'Price_4l'])
price_usd = price_usd.set_index('ID')  # Установка индекса

def export_code_to_id_list():
    if connection.is_connected():
        print('Соединение с базой данных MySQL успешно установлено.')

        # Очистка таблицы id_list
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE id_list")
        connection.commit()
        print('Таблица id_list очищена.')

        # Открытие файла CSV
        with open('code.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)

            # Пропуск заголовка (если есть)
            next(csv_reader)

            # Цикл по строкам CSV-файла
            for row in csv_reader:
                # Извлечение значений из строки
                values = [int(value) if value != '' else None for value in row]

                # Создание SQL-запроса для вставки данных в таблицу
                sql = "INSERT INTO id_list (code_zero, code_1, code_2, code_3, code_4, code_5, code_6, code_7) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

                # Выполнение SQL-запроса с передачей значений
                cursor.execute(sql, values)
                connection.commit()
    # connection.close()


def check_sun_price():
    if connection.is_connected():
        download_price_sun()
        print('Соединение с базой данных MySQL успешно установлено.')
        # Чтение данных из файла Excel
        df = pd.read_excel('sun_notebook_parts.xls')
        df = df.iloc[10:, :]

        # Оставляем только нужные столбцы (ID и Price)
        df = df[['Unnamed: 5', 'Unnamed: 9']].rename(columns={'Unnamed: 5': 'ID', 'Unnamed: 9': 'Price'})

        # Сортируем данные по ID
        df = df.sort_values('ID')

        for _, row in df.iterrows():
            id_to_find = row['ID']
            price_to_save = row['Price']
            if pd.notna(id_to_find):  # Проверка, что значение 'code_1' не равно NaN
                sql = f"SELECT code_zero FROM id_list WHERE code_1 = {id_to_find}"
                cursor = connection.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    code_zero = result[0][0]
                    price_usd.loc[code_zero, 'Price_sun'] = price_to_save

        # Вывод полученных данных
        return price_usd
    # connection.close()
    print('Соединение с базой данных MySQL закрыто.')


def check_dfi_price():
    if connection.is_connected():
        print('Соединение с базой данных MySQL успешно установлено.')

    # Чтение XML файла
    tree = ET.parse('xmlForClients.xml')
    root = tree.getroot()

    # Создание пустого DataFrame
    df = pd.DataFrame(columns=['ID', 'Price'])
    current_rate = get_current()
    # Итерация по элементам <item> внутри <Warehouse>
    for item in root.find('Warehouse').findall('item'):
        available = item.find('Available').text
        if available == 'true':
            item_id = item.find('ItemId').text
            price = item.find('Price').text
            if current_rate:
                price = float(price) / current_rate
                price = round(price, 2)

            # Добавление данных в DataFrame
            df = pd.concat([df, pd.DataFrame({'ID': [item_id], 'Price': [price]})], ignore_index=True)


    # Поиск каждого ID в таблице id_list и сохранение соответствующих пар
    for _, row in df.iterrows():
        id_to_find = row['ID']
        price_to_save = row['Price']
        if pd.notna(id_to_find):  # Проверка, что значение 'code_1' не равно NaN
            sql = f"SELECT code_zero FROM id_list WHERE code_2 = {id_to_find}"
            cursor = connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            if result:
                code_zero = result[0][0]
                price_usd.loc[code_zero, 'Price_dfi'] = price_to_save

    # Закрытие соединения с базой данных
    # connection.close()
    print('Соединение с базой данных MySQL закрыто.')

    # Вывод полученных данных
    return price_usd


def check_4l_price():
    if connection.is_connected():
        print('Соединение с базой данных MySQL успешно установлено.')
    download_price_4l()
    tree = ET.parse('google.xml')
    root = tree.getroot()

    # Создание пустого DataFrame
    current_rate = get_current()
    df = pd.DataFrame(columns=['ID', 'Price'])
    namespaces = {'g': 'http://base.google.com/ns/1.0', 'atom': 'http://www.w3.org/2005/Atom'}

    for entry in root.findall('atom:entry', namespaces):
        availability = entry.find('g:availability', namespaces).text
        if availability == 'in stock':
            item_id = entry.find('g:id', namespaces).text
            price = entry.find('g:price', namespaces).text
            if current_rate:
                price = float(price.split()[0]) / current_rate
                price = round(price, 2)

            df = pd.concat([df, pd.DataFrame({'ID': [item_id], 'Price': [price]})], ignore_index=True)


    # Поиск каждого ID в таблице id_list и сохранение соответствующих пар
    for _, row in df.iterrows():
        id_to_find = row['ID']
        price_to_save = row['Price']
        if pd.notna(id_to_find):
            sql = f"SELECT code_zero FROM id_list WHERE code_5 = {id_to_find}"
            cursor = connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            if result:
                code_zero = result[0][0]
                price_usd.loc[code_zero, 'Price_4l'] = price_to_save

    # Закрытие соединения с базой данных
    # connection.close()
    print('Соединение с базой данных MySQL закрыто.')

    # Вывод полученных данных
    return price_usd


def check_ak_price():
    download_price_ak()
    if connection.is_connected():
        print('Соединение с базой данных MySQL успешно установлено.')
        # Чтение данных из файла Excel
        df = pd.read_excel('a-class_price.xlsx', engine='openpyxl')

        df = df.iloc[14:, :]

        # Оставляем только нужные столбцы (ID и Price)
        df = df[['Unnamed: 2', 'Unnamed: 7']].rename(columns={'Unnamed: 2': 'ID', 'Unnamed: 7': 'Price'})

        for _, row in df.iterrows():
            id_to_find = row['ID']
            price_to_save = row['Price']
            if pd.notna(id_to_find):  # Проверка, что значение 'code_1' не равно NaN
                sql = f"SELECT code_zero FROM id_list WHERE code_4 = '{id_to_find}'"  # Добавление кавычек
                cursor = connection.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    code_zero = result[0][0]
                    price_usd.loc[code_zero, 'Price_ak'] = price_to_save

        # Вывод полученных данных
        return price_usd

    # connection.close()
    print('Соединение с базой данных MySQL закрыто.')


# export_code_to_id_list()

check_sun_price()
sleep(1)
check_ak_price()
sleep(1)
check_dfi_price()
sleep(1)
check_4l_price()
connection.close()
print(price_usd)
