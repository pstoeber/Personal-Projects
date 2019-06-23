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

def gen_db_conn():
    return pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)

def season_scraper(today):
    start_season = datetime.datetime.strptime('2018-10-01', '%Y-%m-%d').date()
    new_year = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d').date()
    end_season = datetime.datetime.strptime('2019-05-01', '%Y-%m-%d').date()

    if today > start_season and today < new_year:
        return today.year + 1
    elif today >= new_year and today < end_season:
        return today.year
    else:
        return 2019

def get_conference(soup):
    return [i.text for i in soup.findAll(True, {'class':['Table2__Title']})]

def parse_series(team):
    index = re.search(r'[A-Z]{3,4}', team).end() -1
    team = team[index:].replace('HUtah', 'Utah')
    return team

def format_df(conf_dict, link, year):
    standing_df = pd.concat([conf_dict['Eastern Conference'], conf_dict['Western Conference']]).rename(columns={0:'team'})
    standing_df.insert(loc=2, column='season', value=year)
    standing_df['team'] = standing_df.team.apply(parse_series)
    standing_df['GB'] = standing_df['GB'].str.replace('-', '0').astype(float)
    standing_df['source_link'] = link
    standing_df['create_at'] = datetime.datetime.now()
    return standing_df

def team_standing_scrap(standing_stats_link, year):
    soup = BeautifulSoup(requests.get(standing_stats_link).content, "html.parser")
    conference_list = get_conference(soup)
    conf_dict = {'Eastern Conference':pd.DataFrame(), 'Western Conference':pd.DataFrame()}
    df_list = pd.read_html(standing_stats_link)
    for c, i in enumerate(range(1, len(df_list), 4)):
        if c < 1:
            conf_dict['Eastern Conference'] = pd.concat([df_list[i], df_list[i+2]], axis=1)
            conf_dict['Eastern Conference'].insert(loc=1, column='conference', value='Eastern Conference')
        else:
            conf_dict['Western Conference'] = pd.concat([df_list[i], df_list[i+2]], axis=1)
            conf_dict['Western Conference'].insert(loc=1, column='conference', value='Western Conference')
    standing_df = format_df(conf_dict, standing_stats_link, year)
    return standing_df

def insert_into_database(df):
    engine = create_engine("mysql+pymysql://", creator=gen_db_conn)
    df.to_sql(con=engine, name='team_standings', if_exists='replace', index=False)
    engine.dispose()
    return

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = gen_db_conn()
    logging.info('Beginning ESPN team standings pipeline {}'.format(str(datetime.datetime.now())))
    year = season_scraper(datetime.date.today())
    standing_stats_link = 'http://www.espn.com/nba/standings/_/season/{year}'.format(year=str(year))
    standings_df = team_standing_scrap(standing_stats_link, str(year))
    insert_into_database(standings_df)
    logging.info('ESPN team standings pipeline completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
