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
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from sqlalchemy import create_engine
from collections import defaultdict

def truncate_tables(conn, table_list):
    for table in table_list:
        truncate = 'truncate table {}'.format(table)
        sql_execute(conn, truncate)

def gen_dates(conn):
    links_list = []
    max_system_date = get_max_date(conn)
    step = datetime.timedelta(days=1)
    start = max_system_date + step
    end = datetime.datetime.today().date()

    while start <= end:
        date = str(start).split('-')
        links_list.append('https://www.basketball-reference.com/boxscores/?month={}&day={}&year={}'.format(date[1], date[2], date[0]))
        start += step
    return links_list

def get_max_date(conn):
    find_max_date = 'select max(game_date) from nba_stats.box_score_map'
    return sql_execute(conn, find_max_date)[0][0]

def get_links(link):
    box_score_list = []
    soup = BeautifulSoup(requests.get(link).content, "html.parser")
    for a in soup.find_all('a', href=True):
        if re.search(r'/boxscores/\d{8}', a['href']):
            link = 'https://www.basketball-reference.com' + a['href']
            box_score_list.append(link)
        else:
            logging.info('[FAILED TO BOXSCORE DATA]: {}'.format(link))
            pass
    return box_score_list

def create_pools(function, iterable):
    pool = Pool()
    results = pool.map(function, iterable)
    pool.close()
    pool.join()
    return results

def box_scrape(page_link):
    tables_df_list = []
    soup = BeautifulSoup(requests.get(page_link).content, "html.parser")
    game_tag, score, game_hash = gen_game_tag_score(soup)
    raw_tables = soup.findAll('table')
    header_list = get_headers(raw_tables)

    for table in raw_tables:
        temp_list = []
        for row in table.findAll('tr'):
            name = row.find('th').text.split('\n')
            row_vals = [i.text for i in row.findAll('td')]
            if len(row_vals) > 1 and name[0] != 'Team Totals':
                row_vals.insert(0, game_hash)
                row_vals.insert(1, ' '.join([i for i in name[:2]]))
                temp_list.append(row_vals)
        tables_df_list.append(pd.DataFrame(np.array(temp_list)))
    return [inject_headers(tables_df_list, header_list), game_tag, score]

def gen_game_tag_score(soup):
    game_tag = soup.find('h1')
    game_hash = hash_gen(soup.find('h1'))
    game_tag = game_tag.text.split(' at ')
    game_tag = list(itertools.chain([game_hash], game_tag[:1], game_tag[1].split(' Box Score, ')))
    score = [i.get_text() for i in soup.findAll(True, {'class':['score']})]
    score = list(itertools.chain([game_hash], score))
    return game_tag, score, game_hash

def hash_gen(game_info_string):
    return hashlib.md5(game_info_string.encode('utf-8')).hexdigest()

def get_headers(raw_tables):
    header_list = []
    for table in raw_tables:
        for header in table.findAll('thead'):
            head = [i.text.replace('%', '_PCT').replace('+/-', 'PLUS_MINUS') for i in header.findAll('th')
                    if i.text not in ['Basic Box Score Stats', 'Starters', 'Advanced Box Score Stats', '']]
            head.insert(0, 'game_hash')
            head.insert(1, 'name')
            header_list.append(head)
    return header_list[:2]

def inject_headers(tables_df_list, header_list):
    df_list = []
    for c, df in enumerate(tables_df_list):
        if (c+1) % 2 != 0:
            df.columns=header_list[0]
            df_list.append(df)
        elif (c+1) % 2 == 0:
            df.columns=header_list[1]
            df_list.append(df)
    return df_list

def create_insert_statements(conn, game_tag, score):
    box_score_map = """insert into nba_stats_staging.box_score_map
                       values("{}", "{}", "{}", str_to_date(\'{}\', \'%M %D %Y\'))""".format(game_tag[0], game_tag[1], game_tag[2], game_tag[3])
    sql_execute(conn, box_score_map)
    game_results = """insert into nba_stats_staging.game_results
                      values ("{}", {}, {})""".format(score[0], score[1], score[2])
    sql_execute(conn, game_results)

def sql_execute(conn, insert_statement):
    exe = conn.cursor()
    exe.execute(insert_statement)
    return exe.fetchall()

def insert_stats(df, table):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))
    df.to_sql(con=engine, name=table, if_exists='append', index=False)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning NBA Reference players incrementals pipeline {}'.format(str(datetime.datetime.now())))
    try:
        myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    except:
        print('Unable to connect to nba_stats_staging environment')

    table_list = ['basic_box_stats', 'advanced_box_stats', 'box_score_map', 'game_results']
    box_score_links = create_pools(get_links, gen_dates(myConnection))
    links = list(set(itertools.chain.from_iterable(box_score_links)))
    truncate_tables(myConnection, table_list)

    info_stats = create_pools(box_scrape, links)
    for content in info_stats:
        create_insert_statements(myConnection, content[-2], content[-1])
        for c, df in enumerate(content[0]):
            if (c+1) % 2 != 0:
                insert_stats(df, table_list[0])
            elif (c+1) % 2 == 0:
                insert_stats(df, table_list[1])
    logging.info('NBA Reference players incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
