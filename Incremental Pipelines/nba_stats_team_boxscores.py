"""
scraper designed to gather team stats from individual games from
stats.nba.com/teams/boxscores-advanced/
"""

import datetime
import numpy as np
import pandas as pd
import pymysql
import requests
import sys
import logging
import time
from sqlalchemy import create_engine
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException

def connect_to_staging():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)

def find_max_date(conn):
    exe = conn.cursor()
    exe.execute('select max(game_date) from nba_stats.box_score_map')
    return exe.fetchall()[0][0]

def create_pools(driver, content):
    pool = Pool(5)
    results = pool.map(partial(stat_scraper, driver=driver), content)
    pool.close()
    pool.join()
    return results

def stat_scraper(link, driver):
    browser = gen_browser(link[1], driver)
    pages = browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[1]/div/div/select')
    options = [x for x in pages.find_elements_by_tag_name("option")]
    truncate_table(link[0])
    for page in range(2, len(options)+1):
        browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[{}]'.format(str(page))).click()
        content = browser.find_element_by_class_name('nba-stat-table').get_attribute('innerHTML')
        df = pd.read_html(content)[0]
        f_df = format_matchup(df)
        f_df['Game Date']  = f_df.loc[:, 'Game Date'].apply(convert_date)
        insert_into_database(f_df[f_df['Game Date'] > link[2]], link[0])
        if f_df['Game Date'].min() <= link[2]:
            break
    browser.quit()
    return

def gen_browser(link, driver):
    options = Options()
    options.accept_untrusted_certs = True
    options.assume_untrusted_cert_issuer = True
    options.add_argument('--load-extension=/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/3.34.0_0')
    browser = webdriver.Chrome(executable_path=driver, chrome_options=options)
    browser.get(link)

    try:
        wait = WebDriverWait(browser, 120).until(EC.presence_of_element_located((By.XPATH, '/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[1]/div/div/select')))
    except TimeoutException or NoSuchElementException:
        browser.refresh()
        logging.info('Failed to connect to page')
    return browser

def truncate_table(table):
    sql = 'truncate table {}'.format(table)
    exe = connect_to_staging().cursor()
    exe.execute(sql)
    return

def format_matchup(df):
    matchup_df = df.iloc[:, :3]
    home_away = np.empty(shape=[1, 4])

    match_up_list = matchup_df.apply(lambda x: parse_teams(x), axis=1)
    for match in match_up_list:
        home_away = np.concatenate([home_away, match], axis=0)

    home_away_df = pd.DataFrame(home_away, index=None, columns=['Team', 'Home_Team', 'Away_Team', 'Game Date'])
    final_df = pd.merge(home_away_df, df, how='inner', on=['Team', 'Game Date'])
    final_df.drop(['Match Up', 'Season'], axis=1, inplace=True)
    return final_df

def parse_teams(row):
    match_up = row.loc['Match Up'].split()
    return_row = []
    if match_up[1] == 'vs.':
        return_row = [i for i in [row.loc['Team'], match_up[0], match_up[2], row.loc['Game Date']]]
        return np.array(return_row).reshape(1,4)
    elif match_up[1] == '@':
        return_row = [i for i in [row.loc['Team'], match_up[2], match_up[0], row.loc['Game Date']]]
        return np.array(return_row).reshape(1,4)

def convert_date(date_str):
    return datetime.datetime.strptime(date_str, '%m/%d/%Y').date()

def insert_into_database(df, table):
    engine = create_engine("mysql+pymysql://", creator=connect_to_staging)
    df.to_sql(con=engine, name=table, if_exists='append', index=False)
    engine.dispose()
    return

def main():
    driver = '/Users/Philip/Downloads/chromedriver 2'
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Starting NBA Stats incrementals pipeline {}'.format(str(datetime.datetime.now())))

    max_date = find_max_date(connect_to_staging())
    content = [['advanced_team_boxscore_stats', 'https://stats.nba.com/teams/boxscores-advanced/', max_date],
               ['figure4_team_boxscore_stats', 'https://stats.nba.com/teams/boxscores-four-factors/', max_date],
               ['team_misc_boxscore_stats', 'https://stats.nba.com/teams/boxscores-misc/', max_date],
               ['team_scoring_boxscore_stats', 'https://stats.nba.com/teams/boxscores-scoring/', max_date],
               ['traditional_team_boxscore_stats', 'https://stats.nba.com/teams/boxscores-traditional/', max_date]]
    create_pools(driver, content)
    logging.info('NBA Stats incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
