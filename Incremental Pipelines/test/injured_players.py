"""

python3 injured_players.py Incremental\ Pipelines/sql\ ddl/active_rosters_player_id.sql

"""

import pymysql
import requests
import sys
import numpy as np
import pandas as pd
import datetime
import logging
from bs4 import BeautifulSoup
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from sqlalchemy import create_engine

def create_threads():
    pool = Pool()
    results = pool.map(extract_injured_players, get_injury_links())
    pool.close()
    pool.join()
    return results

def get_injury_links():
    injuries_links = []
    link = 'http://www.espn.com/nba/teams'
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')

    for i in soup.find_all('a', href=True):
        if '/nba/team/roster/' in i['href']:
            link = i['href'].replace('/roster/', '/injuries/')
            injuries_links.append('https://www.espn.com{}'.format(link))
    return injuries_links

def extract_injured_players(link):
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')
    team_raw = soup.find('h1', class_='headline__h1 dib').text.split()[:-1]
    team = ' '.join([i for i in team_raw])
    injured = soup.findAll(True, {'class':['ContentList']})

    injuries = []
    for i in injured:
        players = i.findAll('h3', class_='di n8')
        status = i.findAll('span', class_='TextStatus TextStatus--red fw-medium ml2')
        bio = i.findAll('div', class_='clr-gray-04 pt3 n8')
        for p, s, b in zip(players, status, bio):
            if s.text in ['Out', 'Suspension']:
                injuries.append([p.text, team])
            elif check_update(b.text):
                injuries.append([p.text, team])
    return np.array(injuries)

def check_update(player_update):
    injured_player_list = []
    ruled_out_list = ['out', 'ruled', 'off', 'miss', 'missed', 'concussion', '(concussion)', 'doubtful']
    for up in player_update.split():
        if up in ruled_out_list:
            return True
    return False

def truncate_table(conn):
    truncate_table_statement = 'truncate table injuries'
    sql_execute(truncate_table_statement, conn)

def extract_command(file_path):
    with open(file_path, 'r') as infile:
        return [i for i in infile.readlines()]

def gen_cmd_str(cmd):
    return ' '.join([i for i in cmd])

def get_player_id(player_name, sql, conn):
    try:
        return sql_execute(sql.format(check_name(player_name)), conn)[0][0]
    except IndexError:
        return 0

def check_name(name):
    if '\'' in name:
        name = name[:name.index('\'')] + '\\' + name[name.index('\''):]
    return name

def insert_into_database(conn, df):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    df.to_sql(con=engine, name='injuries', if_exists='replace', index=False)
    engine.dispose()
    return

def sql_execute(sql, conn):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

def main(arg):
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='w', level=logging.INFO)
    logging.info('Refreshing injured_players table {}'.format(str(datetime.datetime.now())))
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats', autocommit=True)

    results = create_threads()
    players = np.empty(shape=[0, 2])
    for result in results:
        if result.size > 0:
            players = np.concatenate([players, result])

    truncate_table(connection)
    injured_players_df = pd.DataFrame(players, index=None, columns=['name', 'team'])
    injured_players_df['player_id'] = injured_players_df.loc[:, 'name'].astype(str).apply(lambda x: get_player_id(x, gen_cmd_str(extract_command(arg)), connection))
    insert_into_database(connection, injured_players_df[injured_players_df['player_id'] != 0])
    logging.info('Injured_players table refresh complete {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main(sys.argv[1])
