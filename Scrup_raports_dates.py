import pandas as pd
from bs4 import BeautifulSoup
import requests
import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(filename='./logs_scrapper.log', level=logging.DEBUG,
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


try:
    db = mysql.connector.connect(
        user="fellow",
        password="fellow2021",
        host="51.83.129.54",
        port=3306,
        database="fellowshippl"
    )

# -----------------------------------
# Downloading tickers from base
# -----------------------------------

    curs = db.cursor()

    tickers = """SELECT DISTINCT(ticker) FROM financial_calculated_data fcd """

    curs.execute(tickers)

    data = []
    for x in curs:
        data.append(x[0])
        # print(data)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    curs.close()
    db.close()

# -----------------------------------
# Making list of url
# -----------------------------------

page_url = []
for i, ticker in enumerate(data, start=1):
    page = f'https://macronext.pl/pl/kalendarium/{ticker}/'
    page_url.append(page)
print(page_url)

# -----------------------------------
# Scraping data from url and
# dropping to common table
# -----------------------------------

df_general = pd.DataFrame(columns=["Data", 'Spółka', "Wydarzenie"])
for i, page in enumerate(page_url):
    start_url = page
    download_html = requests.get(start_url)
    ticker = page.split("/")[-2]

    soup: BeautifulSoup = BeautifulSoup(download_html.text)

    with open('downloaded.html', 'w', encoding="utf-8") as file:
        file.write(soup.prettify())
    # print(soup.prettify())
    # print(page, i)
    try:
        full_table = soup.select('table.tabdata')[0]
    except IndexError as e:
        logging.error(f'Error occured: {e}')
        print(f"Error occured: {e}")

        print("Continue with the loop")
        continue
    # print(full_table)

    table_head = full_table.select('tr th')
    # print(table_head)

    print("--------------")
    for element in table_head:
        print(element.text)

    import re
    regex = re.compile('_\[ \w ] ')

    table_columns = []
    for element in table_head:
        column_label = element.get_text(separator=" ", strip=True)
        column_label = column_label.replace(' ', '_')
        column_label = regex.sub('', column_label)
        table_columns.append(column_label)
        print(column_label)

    # print('-------------')
    # print(table_columns)

    table_rows = full_table.select('tr')

    table_data = []
    for index, element in enumerate(table_rows):
        if index > 0:
            row_list = []
            values = element.select('td')
            for value in values:
                row_list.append(value.text.strip())
            table_data.append(row_list)

    # print(table_data)

    df = pd.DataFrame(table_data, columns=table_columns)
    # print(df)
df_general.to_csv(f"downloaded_all.csv", sep=";")
