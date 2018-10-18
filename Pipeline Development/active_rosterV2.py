"""
pipeline designed to create/update active roster table within the NBA states database
"""

import pymysql
import re
import requests
import sys
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine

def truncate_table(connection):
    truncate_table_statement = 'truncate table active_rosters'
    sql_execute(truncate_table_statement, connection)

def get_roster_links():
    roster_links = []
    link = 'http://www.espn.com/nba/teams'
    soup = BeautifulSoup(requests.get(link).content, 'html.parser')

    for i in soup.find_all('a', href=True):
        if '/nba/team/roster/' in i['href']:
            roster_links.append('http://www.espn.com{}'.format(i['href']))
    return roster_links

def get_rosters(link, chromeDriver):
    browser = webdriver.Chrome(executable_path=chromeDriver)
    browser.get(link)
    team = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[1]/h1').text.split()[:-1]
    body = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[4]/section/table')

    roster_list = []
    for i in body.text.split('\n')[1:]:
        name = []
        for p in i.split():
            if p not in ['PG', 'SG', 'SF', 'PF', 'C', 'G', 'F']:
                name.append(p)
            else:
                break
        roster_list.append([' '.join([i for i in name[1:]]), ' '.join([i for i in team])])
    browser.quit()
    return np.array(roster_list)

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
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_backup"))
    df.to_sql(con=engine, name='active_rosters', if_exists='replace', index=False)

if __name__ == '__main__':
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_backup", autocommit="true")
    chromeDriver = '/Users/Philip/Downloads/chromedriver'

    truncate_table(myConnection)
    active_rosters = np.empty(shape=[0,2])
    for roster in get_roster_links()[:2]:
        active_rosters = np.concatenate([active_rosters, get_rosters(roster, chromeDriver)])

    rosters_df = pd.DataFrame(active_rosters, index=None, columns=['name', 'team'])
    rosters_df['player_id'] = rosters_df.loc[:, 'name'].astype(str).apply(lambda x: get_player_id(x, gen_cmd_str(extract_command(sys.argv[1])), myConnection))
    team_info_df = gen_df(myConnection, gen_cmd_str(extract_command(sys.argv[2])))

    active_rosters_df = pd.merge(rosters_df[rosters_df['player_id'] != 0], team_info_df, how='inner', left_on='team', right_on='team')
    insert_into_database(myConnection, active_rosters_df)
