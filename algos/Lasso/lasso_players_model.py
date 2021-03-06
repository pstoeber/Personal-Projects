"""
Lasso regression algorithm used to predict individual player scores and the total team score
of nba games_started

python3 lasso_players_model.py home_train.sql away_train.sql lasso_test.sql
"""

import numpy as np
import pandas as pd
import pymysql
import sys
import datetime
from bs4 import BeautifulSoup
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Lasso
from sklearn.linear_model import LassoCV
from sqlalchemy import create_engine

def get_games():
    link = 'http://www.espn.com/nba/schedule'
    raw_schedule = pd.read_html(link)[0]
    schedule = np.empty(shape=[0,2])
    for away, home in zip(raw_schedule.iloc[:, 0], raw_schedule.iloc[:, 1]):
        game = np.array([gen_cmd_str(away.split()[:-1]), gen_cmd_str(home.split()[:-1])]).reshape(1,2)
        schedule = np.concatenate([schedule, game])
    return pd.DataFrame(schedule, index=None, columns=['away', 'home'])

def extract_alpha(conn):
    alpha_query = 'select alpha from lasso_alphas where date = (select max(date) from lasso_alphas) order by home_away desc'
    alphas = execute_sql(conn, alpha_query)
    return [float(alphas[0]['alpha']), float(alphas[1]['alpha'])]

def execute_sql(conn, sql):
    exe = conn.cursor(pymysql.cursors.DictCursor)
    exe.execute(sql)
    return exe.fetchall()

def extract_file(file_path):
    with open(file_path, 'r') as infile:
        return [i for i in infile.readlines()]

def gen_cmd_str(cmd):
    return ' '.join([i for i in cmd])

def gen_df(conn, sql):
    return pd.read_sql(sql=sql, con=conn)

def time_convert(minutes_played):
    time_list = minutes_played.split(':')
    try:
        return ((int(time_list[0]) * 60) + int(time_list[1]))
    except ValueError and IndexError:
        return 0

def days_of_rest(df):
    df['days_of_rest'] = df.game_date.diff().dt.days.fillna(0).astype(int)
    df[df['days_of_rest'] < 0] = 0
    return df

def gen_test_dfs(conn, team_list, test_query):
    df_list = []
    for team in team_list:
        test_df = gen_df(conn, test_query.format(team, team, team, team))
        test_df['minutes_played'] = test_df.loc[:, 'minutes_played'].apply(time_convert)
        test_df = test_df.fillna(0).groupby(['player_id', 'name', 'team'], sort=False)
        max_dates = test_df['game_date'].max().reset_index().loc[:, 'game_date'].apply(lambda x: (datetime.date.today() - x).days)
        df_list.append(pd.concat([test_df.mean().reset_index(), max_dates], axis=1))
    return df_list

def fit_lasso_model(train_df, test_list, alpha):
    lasso = Lasso(alpha)
    train_df = train_df.fillna(0)
    lasso.fit(train_df.loc[:, 'minutes_played':], train_df['pts'])

    for test in test_list:
        pred_df = test.loc[:, :'team']
        predictions = lasso.predict(test.loc[:, 'minutes_played':])
        predictions[predictions < 0] = 0
        pred_df['pts'] = predictions
        pred_df['game_date'] = datetime.date.today()

        r_square = lasso.score(test.loc[:, 'minutes_played':], test['pts'])
        total_pts = np.array([pred_df.iloc[0,2], str(datetime.date.today()), pred_df.iloc[:, -2].sum().astype(float), r_square]).reshape(1,4)
        total_pts_df = pd.DataFrame(total_pts, index=None, columns=['team', 'game_date', 'predicted_total_pts', 'r_squared'])
        insert_into_database(pred_df, 'player_prediction_results')
        insert_into_database(total_pts_df, 'total_points_predictions')
    return

def insert_into_database(df, table_name):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
    engine.dispose()
    return

def main(arg1, arg2, arg3):
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod')
    schedule = get_games()
    train_dict = {'home':gen_cmd_str(extract_file(arg1)), 'away':gen_cmd_str(extract_file(arg2))}
    alphas = extract_alpha(connection)

    for c, (k, v) in enumerate(train_dict.items()):
        train_df = gen_df(connection, v)
        train_df['minutes_played'] = train_df.loc[:, 'minutes_played'].apply(time_convert)
        train_df = days_of_rest(train_df)
        test_query = gen_cmd_str(extract_file(arg3))
        tests = gen_test_dfs(connection, schedule.loc[:, k].tolist(), test_query)
        fit_lasso_model(train_df[train_df.loc[:, 'minutes_played'] >= 360], tests, alphas[c])

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
