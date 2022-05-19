import pymysql
import traceback
import logging
from tqdm import tqdm
import re

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

mysql_code = "XXXXX"


def create_data(episode_id, text_raw):
    min_start = 0

    try:

        for i in range(0, 6):
            for k in range(0, 6):

                if k == 0 and i == 0:
                    continue

                split_regex = '[0' + str(i) + ':' + str(k) + "0";

                text_splitted = text_raw.split(split_regex)

                time_split_data = text_splitted[0]

                min_start = min_start + 1

                if len(time_split_data) > 0:
                    save_to_tbl_transcript_raw(episode_id, min_start * 10 - 10, 10, time_split_data)

                del text_splitted[0]

                text_raw = ''.join(text_splitted)


    except Exception as e:
        print(e)


def save_to_tbl_transcript_raw(episode_id, min_start, time_length, text):
    text = re.sub("([\:]).*?([\]])", "\g<1>\g<2>", text)
    text = re.sub("([\(\[]).*?([\)\]])", "\g<1>\g<2>", text)
    # text = text[8:]
    text = text.replace('[]', '')
    text = text.replace(':]', '')
    text = text.replace('  ,', '')
    text = text.replace("'", "''")
    try:
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=mysql_code,
                               db='mysql')
        cur = conn.cursor()
        mysqlQuery = "INSERT INTO `joe_rogan`.`transcript_time_split` (`episode_id`, `min_start`, `time_length`, `text`) " + \
                     "VALUES ('%s', '%s', '%s', '%s');"
        finalQuery = mysqlQuery % (episode_id, min_start, time_length, text)
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
        "SElECT episode_id, text_raw FROM `joe_rogan`.`transcript` "
        "ORDER BY id desc "
        "LIMIT 1000")
    data = list(cur.fetchall())
    conn.close()
    return data


if __name__ == "__main__":

    unpopulated_records = get_non_populated_records()

    for x in tqdm(unpopulated_records):
        try:
            create_data(x[0], x[1])
        except Exception as e:
            print(e)
