"""
Pipeline usewd to scrap team standings for the last 2 decade of NBA teamsself.
Data will be landed into table TEAM_STANDINGS within the nba_stats databaseself.
"""

import re
import requests
import numpy as np
import pandas as pd
import pymysql
import itertools
import datetime
import logging
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

def season_scraper(today):
    start_season = datetime.datetime.strptime('2018-10-01', '%Y-%m-%d').date()
    new_year = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d').date()
    end_season = datetime.datetime.strptime('2019-05-01', '%Y-%m-%d').date()

    if today > start_season and today < new_year:
        return today.year + 1
    elif today >= new_year and today < end_season:
        return today.year

def team_standing_scrap(standing_stats_link, year):
    soup = BeautifulSoup(requests.get(standing_stats_link).content, "html.parser")
    conference_list = get_conference(soup)
    df_list = []
    for i in pd.read_html(standing_stats_link):
        if i.shape[0] == 15:
             df_list.append(i)
    return format_tables(df_list, conference_list)

def format_tables(df_list, conference_list):
    conf_tables = []
    for i in range(0, len(df_list), 2):
        conf_df = pd.concat([parse_series(df_list[i].iloc[:, 0]), df_list[i+1]], axis=1)
        if i == 0:
            conf_df.insert(loc=1, column='conference', value=conference_list[i])
        elif i == 2:
            conf_df.insert(loc=1, column='conference', value=conference_list[i-1])
        conf_tables.append(conf_df)
    return pd.concat([conf_tables[0], conf_tables[1]])

def parse_series(series):
    team_list = []
    for i in series:
        if re.search(r'[A-Z]{3,4}', str(i)):
            team_list.append(i[re.search(r'[A-Z]{3,4}', i).end() -1:])

    team_df = pd.DataFrame(np.array(team_list), index=None, columns=['team'])
    return team_df

def get_conference(soup):
    return [i.text for i in soup.findAll(True, {'class':['Table2__Title']})]

def insert_into_database(df):
    engine =  create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats_staging"))
    df.to_sql(con=engine, name='team_standings', if_exists='replace', index=False)

def update_statements(connection):
    update_utah = 'update team_standings set team = "Utah Jazz" where team = "HUtah Jazz"'
    sql_execute(update_utah, connection)

def sql_execute(query, connection):
    exe = connection.cursor()
    exe.execute(query)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    logging.info('Beginning ESPN team standings pipeline {}'.format(str(datetime.datetime.now())))

    year = season_scraper(datetime.date.today())
    standing_stats_link = 'http://www.espn.com/nba/standings/_/season/' + str(year)
    standings_df = team_standing_scrap(standing_stats_link, str(year))
    standings_df.insert(loc=2, column='season', value=year)
    standings_df['GB'] = standings_df['GB'].str.replace('-', '0').astype(float)

    insert_into_database(standings_df)
    update_statements(myConnection)
    logging.info('ESPN team standings pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
