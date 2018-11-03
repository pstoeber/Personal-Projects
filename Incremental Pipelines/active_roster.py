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
from functools import partial
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine

def create_threads(driver):
    pool = Pool()
    results = pool.map(partial(get_rosters, driver=driver), get_roster_links())
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

def get_rosters(link, driver):
    options = Options()
    options.headless = True
    options.add_extensions = '/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/3.34.0_0'
    browser = webdriver.Chrome(executable_path=driver, chrome_options=options)
    browser.get(link)

    while True:
        try:
            wait = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, 'fittPageContainer')))
            break
        except TimeoutException or NoSuchElementException:
            browser.refresh()
            logging.info('Failed to connect to page')

    team = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[1]/div/section/section/div[1]/h1').text.split()[:-1]
    body = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[1]/div/section/section/div[4]/section/table')
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

def main(arg1, arg2):
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Refreshing active_rosters table {}'.format(str(datetime.datetime.now())))
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats", autocommit="true")
    #driver = '/Users/Philip/Downloads/geckodriver'
    driver = '/Users/Philip/Downloads/chromedriver 2'

    results = create_threads(driver)
    active_rosters = np.empty(shape=[0,2])
    for roster in results:
        active_rosters = np.concatenate([active_rosters, roster])

    truncate_table(myConnection)
    rosters_df = pd.DataFrame(active_rosters, index=None, columns=['name', 'team'])
    rosters_df['player_id'] = rosters_df.loc[:, 'name'].astype(str).apply(lambda x: get_player_id(x, gen_cmd_str(extract_command(arg1)), myConnection))
    team_info_df = gen_df(myConnection, gen_cmd_str(extract_command(arg2)))

    active_rosters_df = pd.merge(rosters_df[rosters_df['player_id'] != 0], team_info_df, how='inner', left_on='team', right_on='team')
    insert_into_database(myConnection, active_rosters_df)
    logging.info('Active_rosters table refresh complete {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
