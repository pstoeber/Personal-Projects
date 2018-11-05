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
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException

def find_max_date(conn):
    exe = conn.cursor()
    exe.execute('select max(game_date) from nba_stats.box_score_map')
    return exe.fetchall()[0][0]

def stat_scraper(link, driver):
    options = Options()
#    options.headless = True
    options.add_extensions = '/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/3.34.0_0'
    browser = webdriver.Chrome(executable_path=driver, chrome_options=options)
    browser.get(link)

    while True:
        try:
            wait = WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, '/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[2]/div[1]')))
            break
        except TimeoutException or NoSuchElementException:
            browser.refresh()
            print('failed to find page')
            logging.info('Failed to connect to page')


    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]').click()
    browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[1]').click()
    table = browser.find_element_by_class_name('nba-stat-table')
    content = table.get_attribute('innerHTML')
    df = pd.read_html(content)[0]
    browser.quit()
    return format_matchup(df)

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

def insert_into_database(df, max_date):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))
    df[df.loc[:, 'Game Date'] > max_date].to_sql(con=engine, name='figure4_team_boxscore_stats', if_exists='replace', index=False)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
    driver = '/Users/Philip/Downloads/chromedriver 2'
    link = 'https://stats.nba.com/teams/boxscores-four-factors/'
    logging.info('Beginning NBA Stats four factors Team Stats incrementals pipeline {}'.format(str(datetime.datetime.now())))
    max_date = find_max_date(myConnection)

    stat_df = stat_scraper(link, driver)
    stat_df['Game Date'] = stat_df.loc[:, 'Game Date'].apply(convert_date)

    if stat_df[stat_df.loc[:, 'Game Date'] > max_date].empty:
        print('No new data.')
        sys.exit(1)

    insert_into_database(stat_df, max_date)
    logging.info('Four Factors Dataframe Count: {}'.format(str(stat_df.count())))
    logging.info('NBA Stats four factors incrementals pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
