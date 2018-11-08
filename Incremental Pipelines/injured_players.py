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
    results = pool.map(partial(extract_injured_players, driver=driver), get_injury_links())
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

def extract_injured_players(link, driver):
    options = Options()
    options.headless = True
    options.add_argument('--load-extension=/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/3.34.0_0')
    browser = webdriver.Chrome(executable_path=driver, chrome_options=options)
    browser.get(link)
    while True:
        try:
            wait = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="fittPageContainer"]/div[3]')))
            break
        except TimeoutException or NoSuchElementException:
            browser.refresh()
            logging.info('[CONNECTION TIME-OUT]: Re-trying {} at {}'.format(link, str(datetime.datetime.now())))

    team = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[1]/div/section/section/div[1]/h1').text.split()[:-1]
    injured_player_list = []
    for player in browser.find_elements_by_class_name('ContentList')[:-1]:
        player_content = player.text.split('\n')
        if player_content[2] in ['Out', 'Suspension']:
            injured_player_list.append([player_content[0], ' '.join([i for i in team])])
        else:
            if check_update(player_content[3]):
                injured_player_list.append([player_content[0], ' '.join([i for i in team])])
    browser.quit()
    return np.array(injured_player_list)

def check_update(player_update):
    injured_player_list = []
    ruled_out_list = ['out', 'ruled', 'off', 'miss', 'missed', 'concussion', '(concussion)']
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

def sql_execute(sql, conn):
    exe = conn.cursor()
    exe.execute(sql)
    return exe.fetchall()

def main(arg):
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='w', level=logging.INFO)
    logging.info('Refreshing injured_players table {}'.format(str(datetime.datetime.now())))
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats', autocommit=True)
    driver = '/Users/Philip/Downloads/chromedriver 2'

    results = create_threads(driver)
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
