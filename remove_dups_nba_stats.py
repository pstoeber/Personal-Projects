import numpy as np
import pandas as pd
import pymysql
import sys
from sqlalchemy import create_engine

def extract_content(conn, sql):
    return pd.read_sql(sql=sql, con=conn, index_col=None)

def insert_data(conn, df):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    df.to_sql(con=engine, name='advanced_team_boxscore_stats', if_exists='replace', index=False)


if __name__ == '__main__':
    try:
        connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats', autocommit=True)
    except:
        print('Failed to connect to database')
        sys.exit(1)
    misc_sql = 'select * from advanced_team_boxscore_stats'

    misc_df = extract_content(connection, misc_sql)
    misc_df['GAME_DATE'] = pd.to_datetime(misc_df['GAME_DATE']).astype(str)
    misc_df.drop_duplicates(subset=['TEAM', 'HOME_TEAM', 'AWAY_TEAM', 'GAME_DATE'], inplace=True)

    print(misc_df.head())

    insert_data(connection, misc_df)
