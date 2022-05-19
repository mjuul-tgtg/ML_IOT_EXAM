import traceback

import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from string import punctuation
from heapq import nlargest
import pymysql
import pprint
import logging

pp = pprint.PrettyPrinter(indent=4)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

mysql_code = 'XXXXX'


def get_data(id):
    conn = pymysql.connect(host='localhost', user='root',
                           passwd=mysql_code, db='joe_rogan')
    cur = conn.cursor()
    cur.execute(
        "SElECT id, episode_id, text, min_start FROM `transcript_time_split` "
        "WHERE episode_id= " + str(id) +
        " ORDER BY id ASC "
        "LIMIT 10000")
    data_tuple = cur.fetchall()
    cur.close()
    conn.close()

    data = []
    for i in data_tuple:
        data.append(list(i))

    for i in range(len(data)):
        data[i][2] = " ".join(data[i][2].split())

    return data


def get_data_total_episode(id):
    conn = pymysql.connect(host='localhost', user='root',
                           passwd="password", db='joe_rogan')
    cur = conn.cursor()
    cur.execute(
        "SElECT id, episode_id, text_cleaned FROM `transcript` "
        "WHERE episode_id= " + str(id) +
        " ORDER BY id ASC "
        "LIMIT 10000")
    data_tuple = cur.fetchall()
    cur.close()
    conn.close()

    data = []
    for i in data_tuple:
        data.append(list(i))

    for i in range(len(data)):
        data[i][2] = " ".join(data[i][2].split())

    return data


def save_to_db_full(transcript_id, tags, summary):
    try:
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=mysql_code,
                               db='mysql')
        cur = conn.cursor()
        mysqlQuery = "INSERT INTO `joe_rogan`.`transcript_analysis` (`transcript_id`, `tags`, `spacy_summary`,`spacy_fine_tuned` ) " + \
                     "VALUES ('%s', '%s', '%s', 0);"
        finalQuery = mysqlQuery % (transcript_id, tags, summary)
        cur.execute(finalQuery)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error(e)


def summarize(text, per):
    nlp = spacy.load("en_core_web_lg")
    doc = nlp(text)
    tokens = [token.text for token in doc]
    word_frequencies = {}
    for word in doc:
        if word.text.lower() not in list(STOP_WORDS):
            if word.text.lower() not in punctuation:
                if (word.tag_ in ['NN', 'NNS', 'NNP', 'NNPS']):
                    if word.text.lower() not in ['people', 'Joe', 'Rogan']:
                        if word.text not in word_frequencies.keys():
                            word_frequencies[word.text] = 1
                        else:
                            word_frequencies[word.text] += 1
    max_frequency = max(word_frequencies.values())
    for word in word_frequencies.keys():
        word_frequencies[word] = word_frequencies[word] / max_frequency
    sentence_tokens = [sent for sent in doc.sents]
    sentence_scores = {}
    for sent in sentence_tokens:
        for word in sent:
            if word.text.lower() in word_frequencies.keys():
                if sent not in sentence_scores.keys():
                    sentence_scores[sent] = word_frequencies[word.text.lower()]
                else:
                    sentence_scores[sent] += word_frequencies[word.text.lower()]
    select_length = int(len(sentence_tokens) * per)
    summary = nlargest(select_length, sentence_scores, key=sentence_scores.get)
    final_summary = [word.text for word in summary]
    summary = ''.join(final_summary)
    tags = sorted(word_frequencies.items(), key=lambda kv: kv[1], reverse=True)[0:10]

    return tags, summary


def save_to_db(transcript_time_slot_id, tags, summary):
    try:
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=mysql_code,
                               db='mysql')
        cur = conn.cursor()
        mysqlQuery = "INSERT INTO `joe_rogan`.`transcript_time_split_analysis` (`transcript_time_split_id`, `tags`, `spacy_summary`) " + \
                     "VALUES ('%s', '%s', '%s');"
        finalQuery = mysqlQuery % (transcript_time_slot_id, tags, summary)
        cur.execute(finalQuery)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.error(e)


if __name__ == "__main__":

    for x in range(146, 269):
        logging.info("Starting on " + str(x))
        data = get_data_total_episode(x)
        for i in data:
            tempEpisodeID = i[1]
            tempID = i[0]
            tags, summary = summarize(i[2], 0.02)
            summary = summary.replace("'", "''")
            tags_cleaned = "''"
            for tag in tags:
                tags_cleaned = tags_cleaned + tag[0] + "'',''"
            tags_cleaned = tags_cleaned[:-3]
            save_to_db_full(tempID, tags_cleaned, summary)
