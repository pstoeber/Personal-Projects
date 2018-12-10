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
    exe.execute('select max(game_date) from nba_stats.player_misc_stats') #change to new table once in place
    return exe.fetchall()[0][0]

def gen_pools(content, driver):
    pool = Pool(3)
    results = pool.map(partial(stat_scraper, driver=driver), content)
    pool.close()
    pool.join()
    return

def stat_scraper(link_list, driver):
    browser = gen_browser(link_list[0], driver)
    pages = browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[1]/div/div/select')
    options = [x for x in pages.find_elements_by_tag_name("option")]
    truncate_table(link_list[1])
    for page in range(2, len(options)+1):
        browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[{}]'.format(str(page))).click()
        content = browser.find_element_by_class_name('nba-stat-table').get_attribute('innerHTML')
        df = pd.read_html(content)[0]
        f_df = format_matchup(df)
        f_df['Game Date']  = f_df.loc[:, 'Game Date'].apply(convert_date)
        insert_into_database(f_df[f_df['Game Date'] > link_list[2]], link_list[1])
        if f_df['Game Date'].min() <= link_list[2]:
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
        logging.info('[FAILED TO CONNECT TO PAGE]: {}'.format(link))
    return browser

def truncate_table(table):
    sql = 'truncate table {}'.format(table)
    exe = connect_to_staging().cursor()
    exe.execute(sql)
    return

def format_matchup(df):
    matchup_df = df.iloc[:, :4]
    home_away = np.empty(shape=[1, 5])

    match_up_list = matchup_df.apply(lambda x: parse_teams(x), axis=1)
    for match in match_up_list:
        home_away = np.concatenate([home_away, match], axis=0)

    home_away_df = pd.DataFrame(home_away, index=None, columns=['Player', 'Team', 'Home_Team', 'Away_Team', 'Game Date'])

    final_df = pd.merge(home_away_df, df, how='inner', on=['Player', 'Team', 'Game Date'])
    final_df.drop(['Match Up', 'Season'], axis=1, inplace=True)
    return final_df

def parse_teams(row):
    match_up = row.loc['Match Up'].split()
    return_row = []
    if match_up[1] == 'vs.':
        return_row = [i for i in [row.loc['Player'], row.loc['Team'], match_up[0], match_up[2], row.loc['Game Date']]]
        return np.array(return_row).reshape(1,5)
    elif match_up[1] == '@':
        return_row = [i for i in [row.loc['Player'], row.loc['Team'], match_up[2], match_up[0], row.loc['Game Date']]]
        return np.array(return_row).reshape(1,5)

def convert_date(date_str):
    return datetime.datetime.strptime(date_str, '%m/%d/%Y').date()

def insert_into_database(df, table):
    engine = create_engine("mysql+pymysql://", creator=connect_to_staging)
    df.to_sql(con=engine, name=table, if_exists='append', index=False)
    engine.dispose()
    return

def main():
    driver = '/Users/Philip/Downloads/chromedriver 2'
    max_date = find_max_date(connect_to_staging())
    content = [['https://stats.nba.com/players/boxscores-misc', 'player_misc_stats', max_date],
               ['https://stats.nba.com/players/boxscores-scoring', 'player_scoring_stats', max_date],
               ['https://stats.nba.com/players/boxscores-usage', 'player_usage_stats', max_date]]
    gen_pools(content, driver)

if __name__ == '__main__':
    main()
