import requests
import datetime
import numpy as np
import pandas as pd
import re
import pymysql
import itertools
import logging
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

def find_player_id(name, team, exp, cols, index):
    conn = gen_db_conn()
    try:
        player_id = sql_execute(conn, 'select player_id from nba_stats.player_info where name like \'{}\''.format(name))[0][0]
        conn.close()
        return player_id, False
    except:
        player_id = sql_execute(conn, 'select max(player_id) from nba_stats.player_info')[0][0] + index
        player_info = np.array([player_id, name, team, exp]).reshape(1,4)
        insert_into_db(pd.DataFrame(player_info, columns=cols), 'player_info')
        conn.close()
        return player_id, True

def get_player_name(soup):
    first_name = soup.find('span', class_='truncate min-w-0 fw-light').text
    last_name = soup.find('span', class_='truncate min-w-0').text
    return '%s %s' % (first_name, last_name)

def get_exp(soup):
    try:
        bio = soup.find("ul", class_="player-metadata").get_text()
        return int(re.search("Experience(.*\d)", bio).group(1)) #extracting years of experience
    except AttributeError:
        return 0

def get_team(soup):
    try:
        return soup.find('li', class_='last').get_text()
    except AttributeError:
        return 'No Team'

def insert_into_db(df, table):
    engine = create_engine('mysql+pymysql://', creator=gen_db_conn)
    df.to_sql(con=engine, name=table, if_exists='append', index=False)
    engine.dispose()
    return

def player_stat_scraper(player):
    soup = BeautifulSoup(requests.get(player.get('link', None), timeout=None).content, "html.parser")
    name = get_player_name(soup).replace('\'', '\\\'')
    table_names = soup.find_all('div', class_='Table2__Title')
    table_names = ['RegularSeason' + i.text.replace(' ', '') for i in table_names]

    player_id, flag = find_player_id(name, get_team(soup), get_exp(soup), player.get('cols', None), player.get('index', None))
    raw_tables = pd.read_html(player.get('link', None))[1:-3]
    for c, i in enumerate(range(0, len(raw_tables), 4)):
        table_name = table_names[c].lower()
        df = pd.concat([raw_tables[i], raw_tables[i+2]], axis=1).iloc[:-1, :]
        df.insert(loc=0, column='player_id', value=player_id)
        if c == 2:
            df.loc[:, 'RAT'].replace({'-':0}, inplace=True)
            df.loc[:, 'AST/TO'].replace({np.inf:0}, inplace=True)
            df.loc[:, 'STL/TO'].replace({np.inf:0}, inplace=True)
        if flag == True:
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
    player_content = [dict(link=link, cols=player_cols, index=c+1) for c, link in enumerate(player_links)]
    player_stats = create_threads(player_stat_scraper, player_content)
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
