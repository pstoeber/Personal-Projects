import requests
import datetime
import numpy as np
import pandas as pd
import re
import pymysql
import itertools
import logging
from bs4 import BeautifulSoup
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from sqlalchemy import create_engine

def find_team_names():
    team_links = []
    link = 'https://www.espn.com/nba/teams'
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')

    for i in soup.find_all('a', href=True):
        if '/nba/team/roster/' in i['href']:
            team_links.append('https://www.espn.com{}'.format(i['href']))
    return team_links

def create_threads(function, iterable):
    pool = Pool()
    results = pool.map(function, iterable)
    pool.close()
    pool.join()
    return results

def player_id_scraper(team_link):
    raw_links, player_links = [], []
    soup = BeautifulSoup(requests.get(team_link).content, "html.parser")

    for i in soup.find_all('a', href=True): #finding all links
        if 'http://www.espn.com/nba/player/_/id/' in i['href']:
            player_links.append(i['href'].replace('/player/', '/player/stats/'))
    player_links = sorted(set(player_links)) #filtering out repeats from the spliced links list
    return player_links

def player_stat_scrapper(player):
    soup = BeautifulSoup(requests.get(player, timeout=None).content, "html.parser")
    name = soup.find("h1").get_text() #finding player name
    exp = get_exp(soup)
    team = get_team(soup)

    if name == None:
        return None

    table_dict = {'dem':[name, team, exp]}
    for df in pd.read_html(player)[1:]:
        table_name = df.iloc[0,0].replace(' ', '')
        df.columns = df.iloc[1]
        df = df.reindex(df.index.drop([0,1])).iloc[:-1, :]
        df.insert(loc=0, column='player_id', value=name)
        table_dict[table_name] = df
    return table_dict

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

def truncate_tables(conn):
    truncate_list = ['player_info', 'RegularSeasonAverages', 'RegularSeasonTotals', 'RegularSeasonMiscTotals']
    for table in truncate_list:
        sql_execute(conn, 'truncate table {}'.format(table))

def find_player_id(conn, name, team, exp, index):
    try:
        player_id = sql_execute(conn, 'select player_id from nba_stats.player_info where name like \'{}\''.format(name_check(name)))[0][0]
        return True, player_id
    except:
        player_id = sql_execute(conn, 'select max(player_id) from nba_stats.player_info')[0][0] + index
        sql_execute(conn, 'insert into player_info values({}, "{}", "{}", {})'.format(str(player_id), name, team, exp))
        return False, player_id

def name_check(name):
    if '\'' in name:
        name = name[:name.index('\'')] + '\\' + name[name.index('\''):]
    return name

def sql_execute(conn, sql):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

def engine(df, player_bool, player_id, table):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))
    df['player_id'] = player_id
    if player_bool:
        insert_into_database(engine, df[df['SEASON'] == '\'18-\'19'], table)
    elif not player_bool and not df.empty:
        insert_into_database(engine, df, table)

def insert_into_database(engine, df, table):
    df.to_sql(con=engine, name=table, if_exists='append', index=False)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning ESPN players incrementals pipeline {}'.format(str(datetime.datetime.now())))

    try:
        myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    except:
        print('Failed to conenct to nba_stats_staging environment')
        sys.exit(1)

    player_links = create_threads(player_id_scraper, find_team_names())
    player_stats = create_threads(player_stat_scrapper, list(itertools.chain.from_iterable(player_links)))
    truncate_tables(myConnection)
    for c, stat in enumerate(player_stats):
        if stat != None:
            player_bool = True
            player_id = 0
            for k, v in stat.items():
                if k == 'dem':
                    player_bool, player_id = find_player_id(myConnection, v[0], v[1], v[2], (c+1))
                else:
                    engine(v, player_bool, player_id, k)

    logging.info('ESPN players incrementals pipeline completed {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
##for testing
#player_links = ["http://www.espn.com/nba/player/stats/_/id/2579458/marcus-thornton", "http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3136776/dangelo-russell"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/6462/marcus-morris"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3213/al-horford"]
#player_links = ["http://www.espn.com/nba/player/stats/_/id/4237/john-wall", "http://www.espn.com/nba/player/stats/_/id/6580/bradley-beal"]  #"6580/bradley-beal",  #4237/john-wall
#player_links = ["http://www.espn.com/nba/player/stats/_/id/3134880/kadeem-allen"]
