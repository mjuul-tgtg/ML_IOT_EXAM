from random import randint
from time import sleep
import requests
from bs4 import BeautifulSoup as bs
import json
import pymysql
import traceback
import logging
from tqdm import tqdm

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

mysql_code = "XXXXX"

baseUrl = "https://www.happyscribe.com/"


def getData(id, url):
    URL = baseUrl + url
    page = requests.get(URL)
    soup = bs(page.content, "html.parser")
    save_to_tbl_transcript_raw(soup, id)


def save_to_tbl_transcript_raw(soup, id):
    body = str(soup).split('"transcript": "')[1].split('"articleBody": "')[0]

    body = body.replace("&#39;", "''")
    body = body.replace('"', '')
    body = body.replace("}", '')

    try:
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=mysql_code,
                               db='mysql')
        cur = conn.cursor()
        mysqlQuery = "INSERT INTO `joe_rogan`.`transcript` (`text_raw`, `episode_id`) " + \
                     "VALUES ('%s', '%s');"
        finalQuery = mysqlQuery % (body, id)
        cur.execute(finalQuery)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error(e)


def get_non_populated_records():
    conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root',
                           passwd=mysql_code, db='mysql')
    cur = conn.cursor()
    cur.execute(
        "SElECT id, transcript_url FROM `joe_rogan`.`episode_info` "
        "WHERE extra_data_added IS NULL "
        "ORDER BY id asc "
        "LIMIT 1000")
    data = list(cur.fetchall())
    conn.close()
    return data


def mark_data_added(id, type):
    try:
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root',
                               passwd=mysql_code, db='mysql')
        cur = conn.cursor()

        query = "UPDATE `joe_rogan`.`episode_info` SET `extra_data_added` = " + str(
            type) + " WHERE (`id` = '%s');"
        final_query = query % (id)

        cur.execute(final_query)
        conn.commit()

        cur.close()
        conn.close()
    except Exception as e:
        logging.info("Error with query for id : " + str(id))
        logging.error(traceback.format_exc())
        logging.error(e)


if __name__ == "__main__":

    unpopulated_records = get_non_populated_records()

    for x in tqdm(unpopulated_records):
        sleep((randint(4, 6)))
        try:
            getData(x[0], x[1])
            mark_data_added(x[0], 1)
        except Exception as e:
            mark_data_added(x[0], 2)
            print(e)
