"""
Cross-Validation of lasso regression
"""

import numpy as np
import pandas as pd
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import datetime
from bs4 import BeautifulSoup
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Lasso
from sklearn.linear_model import LassoCV
from matplotlib.backends.backend_pdf import PdfPages
from sqlalchemy import create_engine

def extract_file(file_path):
    with open(file_path, 'r') as infile:
        return [i.strip('\n') for i in infile]

def gen_cmd_str(cmd):
    return ' '.join([i for i in cmd])

def gen_df(conn, sql):
    return pd.read_sql(sql=sql, con=conn)

def time_convert(minutes_played):
    time_list = minutes_played.split(':')
    try:
        return ((int(time_list[0]) * 60) + int(time_list[1]))
    except ValueError:
        return 0

def get_alphas(df, flag):
    X_test, X_train, y_test, y_train = train_test_split(df.loc[:, 'minutes_played':], df['pts'], test_size=.33)
    alphas = np.logspace(-4, -1, 100)
    scores = np.empty_like(alphas)

    for i, a in enumerate(alphas):
        lasso = Lasso(alpha=a)
        lasso.fit(X_train, y_train)
        scores[i] = lasso.score(X_test, y_test)

    cv_score, cv_alpha = cross_validate(df.loc[:, 'minutes_played':], df['pts'], alphas)
    plot_alphas(alphas, scores, cv_score, cv_alpha, flag)
    date = datetime.date.today()
    if np.amax(scores) > cv_score:
        return np.array([float(np.take(alphas, np.argmax(scores))), float(np.amax(scores)), flag, date])
    else:
        return np.array([float(cv_alpha), float(cv_score), flag, date])

def cross_validate(X, y, alphas):
    lasso_cv = LassoCV(alphas=alphas)
    lasso_cv.fit(X, y)
    return lasso_cv.score(X, y), lasso_cv.alpha_

def insert_into_database(row):
    cols = ['alpha', 'r_squared', 'home_away', 'date']
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    pd.DataFrame(row.reshape(1,4), columns=cols).to_sql(con=engine, name='lasso_alphas', if_exists='append', index=False)

def plot_alphas(alphas, scores, cv_score, cv_alpha, flag):
    plt.figure()
    plot = plt.plot(alphas, scores, '-ko')
    plot = plt.axhline(cv_score, color='b',ls='--')
    plot = plt.axvline(cv_alpha, color='r', ls='--')
    plot = plt.xlabel('alpha')
    plot = plt.ylabel('score')
    plot = plt.xscale('log')
    plot = sns.despine(offset=15)
    pdf = PdfPages('{}_cross_validation_plots.pdf'.format(flag))
    pdf.savefig(plot)
    pdf.close()

if __name__ == '__main__':
    try:
        connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod')
    except:
        print('Failed to connect to nba_stats_prod environment')
        sys.exit(1)

    home_test_query = gen_cmd_str(extract_file(sys.argv[1]))
    away_test_query = gen_cmd_str(extract_file(sys.argv[2]))
    home_test_df = gen_df(connection, home_test_query)
    away_test_df = gen_df(connection, away_test_query)
    home_test_df.loc[:, 'minutes_played'] = home_test_df.loc[:, 'minutes_played'].apply(time_convert)
    away_test_df.loc[:, 'minutes_played'] = away_test_df.loc[:, 'minutes_played'].apply(time_convert)
    home_alpha = get_alphas(home_test_df[home_test_df['minutes_played'] >= 720].loc[:, 'pts':].fillna(0), 'home')
    away_alpha = get_alphas(away_test_df[away_test_df['minutes_played'] >= 720].loc[:, 'pts':].fillna(0), 'away')
    insert_into_database(home_alpha)
    insert_into_database(away_alpha)
