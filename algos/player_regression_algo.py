"""
Regression algorithm used to predict the number of points each player on a given team will score
against a specific opponent.  Data pulled from nba_stats_prod mysql database instance.
"""

import numpy as np
import pandas as pd
import pymysql
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression

def extract_query(file):
    with open(file, 'r') as infile:
        return [i.strip() for i in infile.readlines()]

def gen_df(conn, sql):
    return pd.read_sql(sql=sql, con=conn)

def time_convert(minutes_played):

    time_list = minutes_played.split(':')
    try:
        return ((int(time_list[0]) * 60) + int(time_list[1]))
    except ValueError:
        return 0

def gen_dummby_var(df, column):
    return pd.get_dummies(df[column], drop_first=True)

def concat_drop(df, dummy_var_col, drop_list):
    concat_df = pd.concat([df, gen_dummby_var(df, dummy_var_col)], axis=1)
    concat_df.drop(drop_list, axis=1, inplace=True)
    return concat_df

def train_split(X, y):
    return train_test_split(X, y, test_size=.33)

def gen_lin_reg_coef(X_train, X_test, y_train, y_test):

    lm = LinearRegression()
    lm.fit(X_train, y_train)
    predictions = lm.predict(X_test)
    #plot_test_data(predictions, y_test)
    print('r_squared value = {}'.format(lm.score(X_test, y_test)))
    return pd.DataFrame(lm.coef_,X_test.columns,columns=['Coefficient']), lm.intercept_.astype(float)

def calc_player_score(coef_df, intercept, player_stats):

    total = 0
    for c, s in zip(coef_df.iloc[0, :], player_stats):
        total += (c * s)
    if player_stats[player_stats['minutes_played'] < 120]:
        total -= 1.0
    return (total + intercept)  # * (9/10)

def gen_log_coef(X_train, X_test, y_train, y_test):
    lg = LogisticRegression()
    lg.fit(X_train, y_train)
    return lg.predict_proba(X_test)

def plot_test_data(predictions, y_test):

    sns.set_style('whitegrid')

    plt.subplot(1, 3, 1)
    plt.scatter(y_test, predictions)
    plt.title('linear regression model')
    plt.xlabel('y_train')
    plt.ylabel('predictions')

    z = np.polyfit(y_test, predictions, 1)
    p = np.poly1d(z)
    plt.plot(y_test,p(y_test),"r")

    plt.subplot(1, 3, 2)
    sns.residplot(x=y_test, y=predictions)
    plt.title('Residuals')

    plt.subplot(1, 3, 3)
    sns.distplot(y_test-predictions, bins=50)
    plt.title('Distribution Plot')

    plt.show()

if __name__ == '__main__':

    #test_roster = ['John Wall',
    #               'Markieff Morris',
    #               'Bradley Beal',
    #               'Tomas Satoransky',
    #               'Mike Scott',
    #               'Time Frazier',
    #               'Otto Porter Jr.',
    #               'Kelly Oubre Jr.',
    #               'Devin Robinson',
    #               'Chris McCullough',
    #               'Marcin Gortat',
    #               'Ian Mahinmi',
    #               'Ramon Sessions',
    #               'Jason Smith',
    #               'Jodie Meeks']

    team_list = ['Chicago', 'Minnesota']

    bulls_roster = ['David Nwaba',
                    'Lauri Markkanen',
                    'Zach LaVine'
                    'Chirstiano Felicio',
                    'Kris Dunn',
                    'Denzel Valentine',
                    'Bobby Portis',
                    'Cameron Payne',
                    'Noah Vonleh',
                    'Jerian Grant',
                    'Omer Asik']

    twolves_roser = ['Karl-Anthony Towns',
                     'Taj Gibson',
                     'Andrew Wiggins',
                     'Jeff Teague',
                     'Nemanja Bjelica',
                     'Jamal Crawford',
                     'Gorgui Dieng',
                     'Tyus Jones',
                     'Marcus Georges-Hunt',
                     'Aaron Brooks',
                     'Cole Aldrich',
                     'Shabazz Muhammad']

    try:
        myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod', autocommit=True)
    except:
        print('Failed to connect to database')
        sys.exit(1)

    train_lin_reg_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[1])]))
    train_lin_reg_df.loc[:, 'minutes_played'] = train_lin_reg_df.loc[:, 'minutes_played'].apply(time_convert)
    test_lin_reg_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[2])]).format('\', \''.join([i for i in test_roster])))
    test_lin_reg_df.loc[:, 'minutes_played'] = test_lin_reg_df.loc[:, 'minutes_played'].apply(time_convert)

    train_lin_reg_df = concat_drop(train_lin_reg_df, 'home_away', ['player_id', 'team', 'game_hash', 'game_date', 'home_away', 'fg', '3p', 'ft'])
    test_lin_reg_df = concat_drop(test_lin_reg_df, 'home_away', ['home_away', 'fg', '3p', 'ft'])

    X_train, X_test, y_train, y_test = train_split(train_lin_reg_df.loc[:, 'minutes_played':'defensive_rating'], train_lin_reg_df.loc[:, 'pts'])
    gen_lin_reg_coef(X_train, X_test, y_train, y_test)

    lin_input_coef, lin_intercept = gen_lin_reg_coef(train_lin_reg_df.loc[:, 'minutes_played':'defensive_rating'], \
                                             test_lin_reg_df.loc[:, 'minutes_played':'defensive_rating'], \
                                             train_lin_reg_df.loc[:, 'pts'], \
                                             test_lin_reg_df.loc[:, 'pts'])

    total_points = test_lin_reg_df.groupby(['player_id', 'name', 'team']).mean()\
                   .loc[:, 'minutes_played':'defensive_rating']\
                   .apply(lambda x: calc_player_score(lin_input_coef.T, lin_intercept.item(), x), axis=1)\
                   .to_frame('pts').reset_index()

    print(total_points)
    print('Total Points {}'.format(total_points.iloc[:, -1].sum()))

    train_log_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[3])]))
    test_log_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[4])]))

    train_log_df = concat_drop(train_log_df, 'home_away', ['home_away'])
    train_log_df = concat_drop(train_log_df, 'win_lose', ['win_lose'])
    test_log_df = concat_drop(test_log_df, 'home_away', ['home_away'])
    test_log_df = concat_drop(test_log_df, 'win_lose', ['win_lose'])

    test_log_df = test_log_df.mean().to_frame().T

    X_train, X_test, y_train, y_test = train_split(train_log_df.drop('W', axis=1), train_log_df.loc[:, 'W'])
    gen_log_coef(X_train, X_test, y_train, y_test)

    win_prob = gen_log_coef(train_log_df.drop('W', axis=1), test_log_df.drop('W', axis=1), \
                                         train_log_df.loc[:, 'W'], test_log_df.loc[:, 'W'])

    print(win_prob)
