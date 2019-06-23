#!/usr/local/bin/python3

import requests
import datetime
import numpy as np
import pandas as pd
import re
import pymysql
import itertools
import logging
import hashlib
from bs4 import BeautifulSoup
from multiprocessing import set_start_method
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from sqlalchemy import create_engine

def gen_timestamp():
    return str(datetime.datetime.now())

def gen_db_conn():
    return pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)

def get_player_info_header(conn):
    sql = 'desc player_info'
    cols = pd.read_sql(sql=sql, con=conn).Field.values.tolist()
    return cols

def find_team_names():
    team_links = []
    link = 'https://www.espn.com/nba/teams'
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')

    for i in soup.find_all('a', href=True):
        if '/nba/team/roster/' in i['href']:
            team_links.append('https://www.espn.com{}'.format(i['href']))
    return team_links

def player_id_scraper(link):
    player_links = []
    soup = BeautifulSoup(requests.get(link).content, "html.parser")

    for c, i in enumerate(soup.find_all('a', href=True)): #finding all links
        name = i.text.replace(' ', '-').lower()
        link = i['href'].replace('/player/', '/player/stats/')
        if re.search(r"http://www.espn.com/nba/player/stats/_/id/[0-9]*/[a-z-]*", link):
            player_links.append(link)
    player_links = sorted(set(player_links)) #filtering out repeats from the spliced links list
    return player_links

def sql_execute(conn, sql):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

def truncate_tables(conn):
    truncate_list = ['player_info', 'RegularSeasonAverages', 'RegularSeasonTotals', 'RegularSeasonMiscTotals']
    for table in truncate_list:
        sql_execute(conn, 'truncate table {};'.format(table))
    return

def get_player_name(soup):
    first_name = soup.find('span', class_='truncate min-w-0 fw-light').text
    last_name = soup.find('span', class_='truncate min-w-0').text
    return '%s %s' % (first_name, last_name)

def get_postion(soup):
    bio = soup.find("div", class_="PlayerHeader__Team n8 mt3 mb4 flex items-center clr-gray-01 mt3 mb4").get_text()
    position = bio.split('#')[-1]
    return re.search(r'[A-Za-z].*', position).group(0)

def get_team(soup):
    try:
        team = soup.find('li', class_='truncate min-w-0').text
    except AttributeError:
        team = 'None'
    return team

def gen_row_hash(row):
    return hashlib.md5(''.join(str(i) for i in row).encode('utf-8')).hexdigest()

def insert_into_db(df, table):
    engine = create_engine('mysql+pymysql://', creator=gen_db_conn)
    df.to_sql(con=engine, name=table, if_exists='append', index=False)
    engine.dispose()
    return

def gen_player_info(name, team, pos, link):
    player_info_list = [name, team, pos, link, gen_timestamp()]
    player_id = gen_row_hash(player_info_list)
    player_info_list.insert(0, player_id)
    player_info = np.array([player_info_list]).reshape(1,6)
    return player_info, player_id

def create_player_info(name, team, pos, link):
    conn = gen_db_conn()
    cols = ['player_id', 'name', 'team', 'position', 'source_link', 'created_at']
    flag = False
    sql = 'select player_id from nba_stats.player_info where name like \'{name}\''.format(name=name.replace('\'', '\\\''))

    try:
        player_id = sql_execute(conn, sql)[0][0]
        flag = True
        conn.close()
    except IndexError:
        player_info, player_id = gen_player_info(name, team, pos, link)
        player_df = pd.DataFrame(player_info, columns=cols, index=None)
        insert_into_db(player_df, 'player_info')
    return player_id, flag

def player_stat_scraper(link):
    soup = BeautifulSoup(requests.get(link).content, "html.parser")
    name = get_player_name(soup)
    table_names = soup.find_all('caption', class_='Table2__Title')
    table_names = [i.text.replace(' ', '') for i in table_names]
    team = get_team(soup)
    pos = get_postion(soup)

    player_id, flag = create_player_info(name, team, pos, link)
    raw_tables = pd.read_html(link)[1:-3]
    for c, i in enumerate(range(0, len(raw_tables), 4)):
        table_name = table_names[c].lower()
        df = pd.concat([raw_tables[i], raw_tables[i+2]], axis=1).iloc[:-1, :]
        df.insert(loc=0, column='player_id', value=player_id)
        df['created_at'] = gen_timestamp()
        if c == 2:
            df.loc[:, 'RAT'].replace({'-':0}, inplace=True)
            df.loc[:, 'AST/TO'].replace({np.inf:0}, inplace=True)
            df.loc[:, 'STL/TO'].replace({np.inf:0}, inplace=True)
        if flag == False:
            insert_into_db(df, table_name)
        else:
            insert_into_db(df[df['season'] == '2018-19'], table_name)
    return

def create_threads(function, iterable):
    pool = Pool(16)
    results = pool.map(function, iterable)
    pool.close()
    pool.join()
    return results

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning ESPN players incrementals pipeline {}'.format(gen_timestamp()))
    set_start_method('forkserver', force=True)
    conn = gen_db_conn()
    player_cols = get_player_info_header(conn)
    player_links = create_threads(player_id_scraper, find_team_names())
    player_links = list(itertools.chain.from_iterable(player_links))
    truncate_tables(conn)
    player_stats = create_threads(player_stat_scraper, player_links)
    logging.info('ESPN players incrementals pipeline completed successfully {}'.format(gen_timestamp()))

if __name__ == '__main__':
    main()

##for testing
#player_links = ["http://www.espn.com/nba/player/stats/_/id/2579458/marcus-thornton", "http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/6462/marcus-morris"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3213/al-horford"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/4237/john-wall", "http://www.espn.com/nba/player/stats/_/id/6580/bradley-beal"]  #"6580/bradley-beal",  #4237/john-wall
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3134880/kadeem-allen"]
