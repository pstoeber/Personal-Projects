"""
script to scrape the content of NBA box scores box scores
"""

import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import pymysql
import itertools
import datetime
import hashlib
import codecs
import sys
import logging
from multiprocessing import Pool
from multiprocessing import set_start_method
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from sqlalchemy import create_engine
from collections import defaultdict

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)

def gen_dates(conn):
    links_list = []
    start = get_max_date(conn)
    end = datetime.datetime.today().date()

    while start <= end:
        date = str(start).split('-')
        links_list.append('https://www.basketball-reference.com/boxscores/?month={}&day={}&year={}'.format(date[1], date[2], date[0]))
        start += datetime.timedelta(days=1)
    return links_list

def get_max_date(conn):
    find_max_date = 'select date_add(max(game_date), interval +1 day) from nba_stats.box_score_map'
    return sql_execute(conn, find_max_date)[0][0]

def get_links(links):
    box_score_list = []
    root_link = 'https://www.basketball-reference.com'
    regex = re.compile(r'/boxscores/\d{8}')
    for link in links:
        soup = BeautifulSoup(requests.get(link).content, "html.parser")
        all_links = soup.find_all('a', href=True)
        box_score_links = [root_link + link['href'] for link in all_links if re.search(r'/boxscores/\d{8}', link['href'])]
        box_score_list.append(list(set(box_score_links)))
    return list(itertools.chain.from_iterable(box_score_list))

def count_threads(link_count):
    threads = 1
    if link_count < 8:
        threads = link_count
    else:
        threads = 8
    return threads

def create_pools(threads, links):
    pool = Pool(threads)
    results = pool.map(box_scrape, links)
    pool.close()
    pool.join()
    return results

def box_scrape(page_link):
    tables_df_list = []
    soup = BeautifulSoup(requests.get(page_link, None).content, "html.parser")
    game_tag, score, game_hash = gen_game_tag_score(soup)

    for table in pd.read_html(page_link):
        df = table.iloc[:-1, :].dropna(axis=1, how='all')
        df.drop(df.index[5], inplace=True)
        df.columns = [col[1].replace('Starters', 'name') for col in df.columns]
        df.insert(loc=1, column='game_hash', value=game_hash)
        tables_df_list.append(df[df['MP'] != 'Did Not Play'])

    return_list = [tables_df_list, pd.DataFrame(game_tag), pd.DataFrame(score)]
    return return_list

def hash_gen(game_info_string):
    return hashlib.md5(game_info_string.encode('utf-8')).hexdigest()

def gen_game_tag_score(soup):
    game_hash = hash_gen(soup.find('h1'))
    game_tag = soup.find('h1')
    game_tag = game_tag.text.split(' at ')
    game_tag = list(itertools.chain([game_hash], game_tag[:1], game_tag[1].split(' Box Score, ')))
    score = [i.get_text() for i in soup.findAll(True, {'class':['score']})]
    score = list(itertools.chain([game_hash], score))
    return np.array(game_tag).reshape(1,4), np.array(score).reshape(1,3), game_hash

def sql_execute(conn, insert_statement):
    exe = conn.cursor()
    exe.execute(insert_statement)
    return exe.fetchall()

def convert_date(date_value):
    return datetime.datetime.strptime(date_value, '%m %d %Y').date()

def insert_stats(df, table):
    engine = create_engine("mysql+pymysql://", creator=gen_db_conn)
    df.to_sql(con=engine, name=table, if_exists='replace', index=False)
    engine.dispose()
    return

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning NBA Reference players incrementals pipeline {}'.format(str(datetime.datetime.now())))
    set_start_method('forkserver', force=True)
    myConnection = gen_db_conn()
    box_score_links = get_links(gen_dates(myConnection))
    threads = count_threads(len(box_score_links))

    table_dict = {'basic_box_stats':pd.DataFrame(),
                  'advanced_box_stats':pd.DataFrame(),
                  'box_score_map':pd.DataFrame(),
                  'game_results':pd.DataFrame()}

    for content, game_tag, score in create_pools(threads, box_score_links):
        game_tag.columns=['game_hash', 'away_team', 'home_team', 'game_date']
        score.columns=['game_hash', 'away_score', 'home_score']
        game_tag['game_date'] = pd.to_datetime(game_tag['game_date']).dt.date
        table_dict['box_score_map'] = pd.concat([game_tag, table_dict['box_score_map']])
        table_dict['game_results'] = pd.concat([score, table_dict['game_results']])
        for c, df in enumerate(content):
            if c % 2 == 0:
                table_dict['basic_box_stats'] = pd.concat([df, table_dict['basic_box_stats']])
            elif c % 2 != 0:
                table_dict['advanced_box_stats'] = pd.concat([df, table_dict['advanced_box_stats']])

    for k, v in table_dict.items():
        insert_stats(v, k)

    logging.info('NBA Reference players incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
