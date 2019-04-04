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
import sys
import logging
from multiprocessing import Pool
from multiprocessing import set_start_method
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from sqlalchemy import create_engine

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_test', autocommit=True)

def gen_days(conn):
    date_list = []
    start = datetime.datetime.strptime('2018-1-1', '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime('2018-12-31', '%Y-%m-%d').date()

    while start <= end_date:
        date_list.append(str(start).split('-')[1:])
        start += datetime.timedelta(days=1)
    return date_list

def sql_execute(conn, sql):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

def find_years(conn):
    get_years = 'select year(game_date) from nba_stats.box_score_map group by year(game_date)'
    years = sql_execute(conn, get_years)
    return [i[0] for i in years]

def gen_dates(dates, year):
    link_list = []
    for date in dates:
        #date = date.split()
        link_list.append('https://www.basketball-reference.com/boxscores/?month={}&day={}&year={}'.format(date[0], date[1], year))
    return list(set(link_list))

def get_links(box_score_links, link):
    box_score_list = []
    root_link = 'https://www.basketball-reference.com'
    regex = re.compile(r'/boxscores/\d{8}')
    #for link in links:
    soup = BeautifulSoup(requests.get(link).content, "html.parser")
    all_links = soup.find_all('a', href=True)
    box_score_links = [root_link + link['href'] for link in all_links if re.search(r'/boxscores/\d{8}', link['href'])]
    if len(box_score_links) > 0:
        box_score_list.append(list(set(box_score_links)))
        return list(itertools.chain.from_iterable(box_score_list))
    else:
        return None
    #return list(itertools.chain.from_iterable(box_score_list))

def count_threads(link_count):
    threads = 1
    if link_count < 8:
        threads = link_count
    else:
        threads = 8
    return threads

def get_header(conn, sql):
    df = pd.read_sql(sql=sql, con=conn, index_col=None)
    return df.loc[:, 'Field'].values.tolist()

def truncate_tables(conn):
    truncate_list = ['basic_box_stats', 'advanced_box_stats', 'box_score_map', 'game_results']
    for table in truncate_list:
        truncate = 'truncate table {table}'.format(table=table)
        sql_execute(conn, truncate)
    return

def hash_gen(game_info_string):
    return hashlib.md5(game_info_string.encode('utf-8')).hexdigest()

def gen_game_tag(header, game_hash, tag_header):
    game_tag = header.text.split(' at ')
    game_tag = list(itertools.chain([game_hash], game_tag[:1], game_tag[1].split(' Box Score, ')))
    game_tag_df = pd.DataFrame(np.array(game_tag).reshape(1,4), columns=tag_header)
    game_tag_df['game_date'] = pd.to_datetime(game_tag_df['game_date']).dt.date
    return game_tag_df

def gen_score_info(soup, game_hash, score_header):
    score = [i.get_text() for i in soup.findAll(True, {'class':['score']})]
    score = list(itertools.chain([game_hash], score))
    return pd.DataFrame(np.array(score).reshape(1,3), columns=score_header)

def insert_stats(stats_content):
    engine = create_engine("mysql+pymysql://", creator=gen_db_conn)
    for k, v in stats_content.items():
        v.to_sql(con=engine, name=k, if_exists='append', index=False)
    engine.dispose()
    return

def box_scrape(content, link):
    stats_content = {'basic_box_stats':pd.DataFrame(), 'advanced_box_stats':pd.DataFrame()}
    soup = BeautifulSoup(requests.get(link, None).content, "html.parser")
    header = soup.find('h1')
    game_hash = hash_gen(header)
    stats_content['box_score_map'] = gen_game_tag(header, game_hash, content.get('tag_header', None))
    stats_content['game_results'] = gen_score_info(soup, game_hash, content.get('score_header', None))
    filter_list = ['Did Not Play', 'Did Not Dress']

    for c, table in enumerate(pd.read_html(link)):
        if c < 2:
            team = stats_content['box_score_map'].iloc[0,1]
        else:
            team = stats_content['box_score_map'].iloc[0,2]
        df = table.iloc[:-1, :].dropna(axis=1, how='all')
        df.drop(df.index[5], inplace=True)
        df.columns = [col[1].replace('Starters', 'name') for col in df.columns]
        df.insert(loc=1, column='game_hash', value=game_hash)
        df.insert(loc=2, column='team', value=team)
        if c % 2 == 0:
            stats_content['basic_box_stats'] = pd.concat([df[~df['MP'].isin(filter_list)].fillna(0), stats_content['basic_box_stats']])
        else:
            stats_content['advanced_box_stats'] = pd.concat([df[~df['MP'].isin(filter_list)].fillna(0), stats_content['advanced_box_stats']])
    insert_stats(stats_content)
    return

def create_pools(threads, function, iterable, static):
    pool = Pool(threads)
    results = pool.map(partial(function, static), iterable)
    pool.close()
    pool.join()
    return results

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning NBA Reference players incrementals pipeline {}'.format(str(datetime.datetime.now())))
    set_start_method('forkserver', force=True)
    conn = gen_db_conn()
    days = gen_days(conn)
    years = find_years(conn)
    links_list = create_pools(16, gen_dates, years, days)
    links_list = list(itertools.chain.from_iterable(links_list))
    real_links = create_pools(32, get_links, links_list, [])

    final_links = []
    for links in real_links:
        if links != None:
            for link in links:
                final_links.append(link)
        else:
            pass
    tag_cols = get_header(conn, 'desc nba_stats.box_score_map')
    score_cols = get_header(conn, 'desc nba_stats.game_results')
    col_info = dict(tag_header=tag_cols, score_header=score_cols)
    #truncate_tables(myConnection)

    create_pools(32, box_scrape, final_links, col_info)
    logging.info('NBA Reference players incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
