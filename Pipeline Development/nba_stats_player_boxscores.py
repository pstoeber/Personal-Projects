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
    exe.execute('select max(game_date) from nba_stats.box_score_map') #change to new table once in place
    return exe.fetchall()[0][0]

def gen_links(current_date):
    link = 'https://stats.nba.com/players/boxscores-misc/?Season={}&SeasonType=Regular%20Season'
    links_list = []
    start = 1996

    if current_date.month > 8:
        end = datetime.date.today().year +1
    else:
        end = datetime.date.today().year

    for year in range(start, end+1):
        link_year = str(year)[:2] + str(int(str(year)[2:])-1).zfill(2) + '-' + str(year)[2:]
        links_list.append([link.format(link_year.replace('20-1', '1999')), 'player_misc_stats'])
    return links_list[1:]

def gen_pools(content, driver):
    pool = Pool()
    results = pool.map(partial(stat_scraper, driver=driver), content)
    pool.close()
    pool.join()
    return

def stat_scraper(link_list, driver):
    #truncate_table(table_name)
    options = Options()
    options.accept_untrusted_certs = True
    options.assume_untrusted_cert_issuer = True
    options.add_argument('--load-extension=/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/3.34.0_0')
    browser = webdriver.Chrome(executable_path=driver, chrome_options=options)
    browser.get(link_list[0])
    #browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/div[1]/div[1]/div/div/label/select/option[1]').click()

    try:
        wait = WebDriverWait(browser, 120).until(EC.presence_of_element_located((By.XPATH, '/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[1]/div/div/select')))
    except TimeoutException or NoSuchElementException:
        browser.refresh()
        #logging.info('Failed to connect to page')

    pages = browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[1]/div/div/select')
    options = [x for x in pages.find_elements_by_tag_name("option")]

    for page in range(2, len(options)+1):
        browser.find_element_by_xpath('/html/body/main/div[2]/div/div[2]/div/div/nba-stat-table/div[3]/div/div/select/option[{}]'.format(str(page))).click()
        table = browser.find_element_by_class_name('nba-stat-table')
        content = table.get_attribute('innerHTML')
        df = pd.read_html(content)[0]
        f_df = format_matchup(df)
        f_df['Game Date']  = f_df.loc[:, 'Game Date'].apply(convert_date)
        insert_into_database(f_df, link_list[1])
    browser.quit()
    return

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

if __name__ == '__main__':
    myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
    myConnection = connect_to_staging()
    driver = '/Users/Philip/Downloads/chromedriver 2'
    content = gen_links(datetime.date.today())
    gen_pools(content, driver)
