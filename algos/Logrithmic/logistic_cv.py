"""
Cross-validation of C hyper-parameter in LogisticRegression
model using LogisticRegressionCV

python3 logistic_cv.py home_train.sql away_train.sql
"""

import sys
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pymysql
from sklearn.model_selection import train_test_split
from sklearn.model_selection import KFold
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LogisticRegressionCV
from matplotlib.backends.backend_pdf import PdfPages
from sqlalchemy import create_engine

def extract_file(file_path):
    with open(file_path, 'r') as infile:
        return [i.strip('\n') for i in infile.readlines()]

def gen_cmd_str(cmd):
    return ' '.join([i for i in cmd])

def gen_df(conn, sql):
    return pd.read_sql(sql=sql, con=conn)

def gen_dummby_var(col):
    return pd.get_dummies(col, drop_first=True)

def opt_c_val(df, flag):
    X_train, X_test, y_train, y_test = train_test_split(df.loc[:, :'opp_pts_scored'], df['w'], test_size=.33)
    c_vals = list(np.power(10.0, np.arange(-10, 10)))
    scores = np.empty_like(c_vals)

    for i, c in enumerate(c_vals):
        lg = LogisticRegression(penalty='l2', C=c)
        lg.fit(X_train, y_train)
        scores[i] = lg.score(X_test, y_test)

    cv_score, cv_c = cross_validate(df.loc[:, :'opp_pts_scored'], df['w'], c_vals)
    date = datetime.date.today()
    plot_c_vals(c_vals, scores, cv_c, cv_score, flag)

    if np.amax(scores) > cv_score:
        return np.array([float(np.take(c_vals, np.argmax(scores))), float(np.amax(scores)), flag, date])
    else:
        return np.array([float(cv_c), float(cv_score), flag, date])

def cross_validate(X, y, Cs):
    folds = KFold(n_splits=5, shuffle=True)
    log_reg_cv = LogisticRegressionCV(Cs=Cs, cv=folds, penalty="l2", scoring='roc_auc',solver='newton-cg', max_iter=10000)
    log_reg_cv.fit(X, y)
    return log_reg_cv.score(X, y), log_reg_cv.C_

def insert_into_database(row):
    cols = ['C_value', 'score', 'home_away', 'date']
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    pd.DataFrame(row.reshape(1,4), columns=cols).to_sql(con=engine, name='c_values', if_exists='append', index=False)

def plot_c_vals(c_vals, scores, cv_c, cv_score, flag):
    plt.figure()
    plt.plot(c_vals, scores, '-ko')
    ax = plt.gca()
    ax = ax.set_xscale('log')
    plot = plt.axhline(cv_score, color='b',ls='--')
    plot = plt.axvline(cv_c, color='r', ls='--')
    plot = plt.xlabel('C value')
    plot = plt.ylabel('Score')
    plot = sns.despine(offset=15)
    pdf = PdfPages('{}_cross_validation_plots.pdf'.format(flag))
    pdf.savefig(plot)
    pdf.close()

if __name__ == '__main__':
    try:
        conn = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod')
    except:
        print('Unable to connect to nba_stats_prod')
        sys.exit(1)

    train_dict = {'home':gen_cmd_str(extract_file(sys.argv[1])), 'away':gen_cmd_str(extract_file(sys.argv[2]))}
    for flag, sql in train_dict.items():
        train_df = gen_df(conn, sql)
        train_df['w'] = gen_dummby_var(train_df.loc[:, 'win_lose'])
        train_df.drop('win_lose', axis=1, inplace=True)
        c_val = opt_c_val(train_df, flag)
        insert_into_database(c_val)
        print(c_val)
