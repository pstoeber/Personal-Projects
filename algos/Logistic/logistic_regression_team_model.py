"""
Logistic Regression algorithm used to predict the Probability of a win or loose
for a team's next game

python3 logistic_regression_team_model.py home_train.sql away_train.sql logistic_test.sql
"""

import sys
import datetime
import numpy as np
import pandas as pd
import pymysql
from bs4 import BeautifulSoup
from sklearn.model_selection import train_test_split
from sklearn.model_selection import KFold
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LogisticRegressionCV
from matplotlib.backends.backend_pdf import PdfPages
from sqlalchemy import create_engine

def get_games():
    link = 'http://www.espn.com/nba/schedule'
    raw_schedule = pd.read_html(link)[0]

    schedule = np.empty(shape=[0,2])
    for away, home in zip(raw_schedule.iloc[:, 0], raw_schedule.iloc[:, 1]):
        game = np.array([format_team(away), format_team(home)]).reshape(1,2)
        schedule = np.concatenate([schedule, game])
    return pd.DataFrame(schedule, index=None, columns=['away', 'home'])

def format_team(team):
    return ' '.join([i for i in team.split()[:-1]])

def get_c_values(conn):
    c_val = 'select C_value from c_values where date = (select max(date) from c_values) order by home_away desc'
    Cs = execute_sql(conn, c_val)
    return [float(Cs[0]['C_value']), float(Cs[1]['C_value'])]

def extract_file(file_path):
    with open(file_path, 'r') as infile:
        return [i.strip('\n') for i in infile.readlines()]

def gen_cmd_str(cmd):
    return ' '.join([i for i in cmd])

def gen_df(conn, sql):
    return pd.read_sql(sql=sql, con=conn)

def gen_dummby_var(col):
    return pd.get_dummies(col, drop_first=True)

def gen_test_dfs(conn, team_list, sql):
    df_list = []
    for team in team_list:
        test_df = gen_df(conn, sql.format(team, team, team))
        df_list.append(test_df.groupby('team').mean().reset_index())
    return df_list

def fit_logistic_model(train_df, tests, C):
    lg = LogisticRegression(penalty='l2', C=C)
    lg.fit(train_df.loc[:, :'opp_pts_scored'], train_df['w'])

    for test in tests:
        win_prob = pd.DataFrame(lg.predict_proba(test.loc[:, 'game_length':]), index=None, columns=['lose_probability', 'win_probability'])
        win_prob['team'] = test.iloc[0, 0]
        win_prob['game_date'] = datetime.date.today()
        insert_into_database(win_prob)
    return

def execute_sql(conn, sql):
    exe = conn.cursor(pymysql.cursors.DictCursor)
    exe.execute(sql)
    return exe.fetchall()

def insert_into_database(df):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    df.to_sql(con=engine, name='win_probability_results', if_exists='append', index=False)

if __name__ == '__main__':
    try:
        connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod')
    except:
        print('Unable to connect to nba_stats_prod environment')
        sys.exit(1)

    schedule = get_games()
    Cs = get_c_values(connection)
    train_dict = {'home':gen_cmd_str(extract_file(sys.argv[1])), 'away':gen_cmd_str(extract_file(sys.argv[2]))}
    test_query = gen_cmd_str(extract_file(sys.argv[3]))

    for c, (k, v) in enumerate(train_dict.items()):
        train_df = gen_df(connection, v)
        train_df['w'] = gen_dummby_var(train_df['win_lose'])
        train_df.drop('win_lose', axis=1, inplace=True)
        tests = gen_test_dfs(connection, schedule.loc[:, k].tolist(), test_query)
        fit_logistic_model(train_df, tests, Cs[c])
