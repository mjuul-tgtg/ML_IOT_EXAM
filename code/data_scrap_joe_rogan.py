from random import randint
from time import sleep
import requests
from bs4 import BeautifulSoup as bs
import pymysql
import traceback
import logging

mysql_code = "XXXX"


def getData(page):
    URL = "https://www.happyscribe.com/public/the-joe-rogan-experience?page=" + str(page)
    page = requests.get(URL)
    soup = bs(page.content, "html.parser")
    episodes = soup.find_all("a", {"class": "hsp-card-episode"})
    for episode in episodes:
        url = episode["href"]
        url = url.replace("'", "''")
        show_name = episode.find_next('h3').text
        show_name = show_name.replace("'", "''")
        show_desc = episode.find_next('p').text
        show_desc = show_desc.replace("'", "''")
        save_to_db(show_name, show_desc, url)


def save_to_db(show_name, show_desc, transcript_url):
    try:
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=mysql_code,
                               db='mysql')
        cur = conn.cursor()
        logging.info("Starting on " + show_name)
        mysqlQuery = "INSERT INTO `joe_rogan`.`episode_info` (`show_name`, `show_desc`, `transcript_url`) " + \
                     "VALUES ('%s', '%s', '%s');"
        finalQuery = mysqlQuery % (show_name, show_desc, transcript_url)
        cur.execute(finalQuery)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error(e)


if __name__ == "__main__":
    for i in range(0, 8):
        sleep(randint(4,6))
        getData(i)
