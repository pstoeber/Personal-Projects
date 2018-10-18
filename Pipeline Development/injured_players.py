"""

python3 injured_players.py Incremental\ Pipelines/sql\ ddl/active_rosters_player_id.sql 

"""

import pymysql
import re
import requests
import sys
import time
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine

def truncate_table(conn):
    truncate_table_statement = 'truncate table injuries'
    sql_execute(truncate_table_statement, connection)

def get_injury_links():
    injuries_links = []
    link = 'http://www.espn.com/nba/teams'
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')

    for i in soup.find_all('a', href=True):
        if '/nba/team/roster/' in i['href']:
            link = i['href'].replace('/roster/', '/injuries/')
            injuries_links.append('http://www.espn.com{}'.format(link))
    return injuries_links

def  extract_injured_players(link):
    chromeDriver = '/Users/Philip/Downloads/chromedriver'
    browser = webdriver.Chrome(executable_path=chromeDriver)
    browser.get(link)
    team = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[1]/h1').text.split()[:-1]

    injured_player_list = []
    injured_players = browser.find_elements_by_class_name('ContentList')  #//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section
    for player in injured_players[:-1]:
        player_content = player.text.split('\n')
        if player_content[2] == 'Out':
            injured_player_list.append([player_content[0], ' '.join([i for i in team])])
        else:
            ruled_out_list = ['out', 'ruled out', 'did not make the trip', 'off', 'night off']
            for crit in ruled_out_list:
                if crit in player_content[3]:
                    injured_player_list.append([player_content[0], ' '.join([i for i in team])])
                    break
    browser.quit()
    return np.array(injured_player_list)

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
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_backup"))
    df.to_sql(con=engine, name='injuries', if_exists='replace', index=False)

def sql_execute(sql, conn):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

def main(arg):
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_backup', autocommit=True)
    truncate_table(connection)
    links = get_injury_links()
    players = np.empty(shape=[0, 2])
    for link in links:
        try:
            players = np.concatenate([players, extract_injured_players(link)])
        except ValueError:
            pass

    injured_players_df = pd.DataFrame(players, index=None, columns=['name', 'team'])
    injured_players_df['player_id'] = injured_players_df.loc[:, 'name'].astype(str).apply(lambda x: get_player_id(x, gen_cmd_str(extract_command(arg)), connection))
    insert_into_database(connection, injured_players_df[injured_players_df['player_id'] != 0])

if __name__ == '__main__':
    main(sys.argv[1])
