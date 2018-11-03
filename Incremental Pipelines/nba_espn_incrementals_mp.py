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
    avg_df, avg_tot_df, misc_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    soup = BeautifulSoup(requests.get(player, timeout=None).content, "html.parser")
    name = soup.find("h1").get_text() #finding player name
    exp = get_exp(soup)
    table_heads, header, stats_raw = get_table_content(soup)

    if len(table_heads) != 3:
        return None

    header_list, table_names = get_table_names(header, table_heads)
    for c, p in enumerate(stats_raw):
        row = ' '.join([str(i.get_text()) for i in p]).split()
        row.insert(0, name)
        if len(row) == len(header_list[0]):
            avg_df = pd.concat([avg_df, pd.DataFrame(np.array(row)).T])
        elif len(row) == len(header_list[1]):
            avg_tot_df = pd.concat([avg_tot_df, pd.DataFrame(np.array(row)).T])
        elif len(row) == len(header_list[2]):
            misc_df = pd.concat([misc_df, pd.DataFrame(np.array(row)).T])

    avg_df.columns=header_list[0]
    avg_tot_df.columns=header_list[1]
    misc_df.columns=header_list[2]
    return [exp, [table_names[0], avg_df], [table_names[1], avg_tot_df], [table_names[2], misc_df]]

def get_exp(soup):
    try:
        bio = soup.find("ul", class_="player-metadata").get_text()
        return int(re.search("Experience(.*\d)", bio).group(1)) #extracting years of experience
    except AttributeError:
        return 0

def get_table_content(soup):
    table_heads = soup.find_all("tr", class_="stathead") #finding the names of tables on web page
    header = soup.find_all("tr", class_="colhead") #finding the header of each table
    stats_raw = soup.findAll(True, {'class':['oddrow', 'evenrow']}) #finding specific stats for seasons
    return table_heads, header, stats_raw

def get_table_names(header, table_heads):
    header_list, table_names = [], []
    for i, n in zip(header, table_heads):
        header = " ".join([j.get_text() for j in i]).split()
        header.insert(0, 'player_id')
        header_list.append(header) #creating list of all headers from each table
        table_names.append(n.get_text()) #creating list to store table names
    return header_list, table_names

def truncate_tables(conn):
    truncate_list = ['player_info', 'RegularSeasonAverages', 'RegularSeasonTotals', 'RegularSeasonMiscTotals']
    for table in truncate_list:
        sql_execute(conn, 'truncate table {}'.format(table))

def find_player_id(conn, df, exp, index):
    find_player_name = df.iloc[0, 0]
    find_player_team = df.iloc[-1, 2]
    try:
        player_id = sql_execute(conn, 'select player_id from nba_stats.player_info where name like \'{}\''.format(name_check(find_player_name)))[0][0]
        return True, player_id
    except:
        player_id = sql_execute(conn, 'select max(player_id) from nba_stats.player_info')[0][0] + index
        sql_execute(conn, 'insert into player_info values({}, "{}", "{}", {})'.format(str(player_id), find_player_name, find_player_team, exp))
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

    for p, player in enumerate(player_stats):
        if player != None:
            player_bool = True
            player_id = ''
            for c, stat in enumerate(player):
                if c == 0:
                    player_bool, player_id = find_player_id(myConnection, player[c+1][1], stat, p)
                else:
                    engine(stat[1], player_bool, str(player_id), stat[0].replace(' ', ''))
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
