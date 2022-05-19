from random import randint
from time import sleep
import json
import pymysql
import traceback
import logging
from tqdm import tqdm
import re

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

mysql_code = "XXXXX"


def clean_data(episode_id, text_raw):
    text_clean = re.sub("([\(\[]).*?([\)\]])", "\g<1>\g<2>", text_raw)
    text_clean = text_clean.replace('[]', '')
    text_clean = text_clean.replace('  ,', '')
    text_clean = text_clean.replace("'", "''")

    add_clean_text_to_tbl(episode_id, text_clean)

def add_clean_text_to_tbl(episode_id, text_clean):
    try:
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root',
                               passwd=mysql_code, db='mysql')
        cur = conn.cursor()

        query = "UPDATE `joe_rogan`.`transcript` SET `text_cleaned` = '%s' WHERE (`episode_id` = '%s');"
        final_query = query % (text_clean ,episode_id)

        cur.execute(final_query)
        conn.commit()

        cur.close()
        conn.close()
    except Exception as e:
        logging.info("Error with query for id : " + str(id))
        logging.error(traceback.format_exc())
        logging.error(e)

def get_non_populated_records():
    conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root',
                           passwd=mysql_code, db='mysql')
    cur = conn.cursor()
    cur.execute(
        "SElECT episode_id, text_raw FROM `joe_rogan`.`transcript` "
        "ORDER BY id asc "
        "LIMIT 1000")
    data = list(cur.fetchall())
    conn.close()
    return data

if __name__ == "__main__":

    unpopulated_records = get_non_populated_records()

    for x in tqdm(unpopulated_records):
        try:
            clean_data(x[0], x[1])
        except Exception as e:
            print(e)
