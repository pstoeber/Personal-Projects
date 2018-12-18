"""
pipeline designed to create/update active roster table within the NBA states database

python3 active_rosterV2.py Incremental\ Pipelines/sql\ ddl/active_rosters_player_id.sql Incremental\ Pipelines/sql\ ddl/active_rosters_team_info.sql
"""

import pymysql
import re
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
    results = pool.map(get_rosters, get_roster_links())
    pool.close()
    pool.join()
    return results

def get_roster_links():
    roster_links = []
    link = 'http://www.espn.com/nba/teams'
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')

    for i in soup.find_all('a', href=True):
        if '/nba/team/roster/' in i['href']:
            roster_links.append('https://www.espn.com{}'.format(i['href']))
    return roster_links

def get_rosters(link):
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')
    team = soup.find("h1", class_="headline__h1 dib").get_text().split()[:-1]

    roster_df = pd.read_html(link)[-1]
    roster_df.insert(loc=2, column='team', value=' '.join([i for i in team]))
    return roster_df.iloc[:, 1:3]

def truncate_table(connection):
    truncate_table_statement = 'truncate table active_rosters'
    sql_execute(truncate_table_statement, connection)

def extract_command(file_path):
    with open(file_path, 'r') as infile:
        return [i for i in infile.readlines()]

def gen_cmd_str(file_content):
    return ' '.join([i for i in file_content])

def gen_df(conn, sql):
    return pd.read_sql(sql=sql, con=conn)

def get_player_id(player_name, sql, conn):
    try:
        return sql_execute(sql.format(check_name(player_name)), conn)[0][0]
    except IndexError:
        return 0

def check_name(name):
    if '\'' in name:
        name = name[:name.index('\'')] + '\\' + name[name.index('\''):]
    return name

def sql_execute(query, connection):
    exe = connection.cursor()
    exe.execute(query)
    return exe.fetchall()

def insert_into_database(conn, df):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    df.to_sql(con=engine, name='active_rosters', if_exists='replace', index=False)
    engine.dispose()
    return

def main(arg1, arg2):
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Refreshing active_rosters table {}'.format(str(datetime.datetime.now())))
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit="true")

    rosters_df = pd.DataFrame()
    results = create_threads()
    for roster in results:
        rosters_df = pd.concat([rosters_df, roster])

    truncate_table(myConnection)
    rosters_df['player_id'] = rosters_df.loc[:, 'Name'].astype(str).apply(lambda x: get_player_id(x, gen_cmd_str(extract_command(arg1)), myConnection))
    team_info_df = gen_df(myConnection, gen_cmd_str(extract_command(arg2)))

    active_rosters_df = pd.merge(rosters_df[rosters_df['player_id'] != 0], team_info_df, how='inner', left_on='team', right_on='team')
    insert_into_database(myConnection, active_rosters_df)
    logging.info('Active_rosters table refresh complete {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
