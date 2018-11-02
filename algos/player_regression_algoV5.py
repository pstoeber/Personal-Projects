"""
Regression algorithm used to predict the number of points each player on a given team will score
against a specific opponent.  Data pulled from nba_stats_prod mysql database instance.

python3 player_regression_algoV5.py train_lin_test.sql train_log_query.sql test_lin_test_game.sql test_log_query.sql

"""

import numpy as np
import pandas as pd
import pymysql
import datetime
#import matplotlib.pyplot as plt
#import seaborn as sns
import sys
import requests
import itertools
#from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sqlalchemy import create_engine

def get_games(driver):
    games_list = []
    link = 'http://www.espn.com/nba/schedule'
    browser = gen_browser(driver)
    browser.get(link)
    games = browser.find_elements_by_xpath('//*[@id="sched-container"]/div[2]/table/tbody/tr')
    for c, ele in enumerate(games):
        away_team = ele.find_element_by_xpath('//*[@id="sched-container"]/div[2]/table/tbody/tr[{}]/td[1]'.format(str(c + 1))).text
        home_team = ele.find_element_by_xpath('//*[@id="sched-container"]/div[2]/table/tbody/tr[{}]/td[2]'.format(str(c + 1))).text
        games_list.append([away_team, home_team])
    browser.quit()
    return list(itertools.chain(*games_list))

def gen_browser(driver):
    options = Options()
    options.headless = True
    options.add_extensions = '/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/3.34.0_0'
    browser = webdriver.Chrome(executable_path=driver, chrome_options=options)
    return webdriver.Chrome(executable_path=driver, chrome_options=options)

def extract_query(file):
    with open(file, 'r') as infile:
        return [i.strip() for i in infile.readlines()]

def sql_execute(conn, query, roster):
    exe = conn.cursor(pymysql.cursors.DictCursor)
    exe.execute(query)
    for row in exe.fetchall():
        roster.append(escape_special_char(row['name']))
    return roster

def escape_special_char(name_string):
    if '\'' in name_string:
        return name_string[:name_string.index('\'')] + '\\' +  name_string[name_string.index('\''):]
    else:
        return name_string

def lin_test(test_df, train_df, team, current_date):
    test_df.loc[:, 'minutes_played'] = test_df.loc[:, 'minutes_played'].apply(time_convert)
    test_df = concat_drop(test_df, ['home_away'], ['home_away', 'fg', '3p', 'ft'])

    lin_input_coef, lin_intercept, r_square = gen_lin_reg_coef(train_df.loc[:, 'minutes_played':'defensive_rating'], \
                                                               test_df.loc[:, 'minutes_played':'defensive_rating'], \
                                                               train_df.loc[:, 'pts'], \
                                                               test_df.loc[:, 'pts'])

    total_points = aggregrate_total_points(test_df, ['player_id', 'name', 'team'], 'minutes_played', 'defensive_rating', \
                                           lin_input_coef.T, lin_intercept.item(), 'pts')
    total_points['game_date'] = str(current_date)
    linear_reg_np_arr = np.array([team, str(current_date), total_points.iloc[:, -2].sum().astype(float), r_square]).reshape(1,4)
    linear_reg_pred_df = pd.DataFrame(linear_reg_np_arr, index=None, columns=['team', 'game_date', 'predicted_total_pts', 'r_squared'])

    insert_into_database(total_points, 'player_prediction_results')
    insert_into_database(linear_reg_pred_df, 'total_points_predictions')
    return total_points, total_points.iloc[:, -2].sum(), r_square

def log_test(test_df, train_df, team, current_date):
    test_df = concat_drop(test_df, ['home_away', 'win_lose'], ['home_away', 'win_lose']).mean().to_frame().T
    win_prob = gen_log_coef(train_df.drop('W', axis=1), test_df.drop('W', axis=1), \
                                         train_df.loc[:, 'W'], test_df.loc[:, 'W'])

    win_prob_df = pd.DataFrame(win_prob, index=None, columns=['lose_probability', 'win_probability'])
    win_prob_df['team'] = team
    win_prob_df['game_date'] = str(current_date)
    insert_into_database(win_prob_df, 'win_probability_results')
    return win_prob

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
    for field in dummy_var_col:
        df = pd.concat([df, gen_dummby_var(df, field)], axis=1)
    df.drop(drop_list, axis=1, inplace=True)
    return df

def train_split(X, y):
    return train_test_split(X, y, test_size=.33)

def gen_lin_reg_coef(X_train, X_test, y_train, y_test):
    lm = LinearRegression()
    lm.fit(X_train, y_train)
    predictions = lm.predict(X_test)
    #plot_test_data(predictions, y_test)
    return pd.DataFrame(lm.coef_, X_test.columns, columns=['Coefficient']), lm.intercept_.astype(float), lm.score(X_test, y_test)

def aggregrate_total_points(df, group_list, slice_st, slice_end, coef_df, intercept, field_name):
    return df.groupby(group_list).mean().loc[:, slice_st:slice_end].apply(lambda x: calc_player_score(coef_df, intercept, x), axis=1).to_frame(field_name).reset_index()

def calc_player_score(coef_df, intercept, player_stats):
    total = 0
    for c, s in zip(coef_df.iloc[0, :], player_stats):
        total += (c * s)
    return total + intercept

def gen_log_coef(X_train, X_test, y_train, y_test):
    lg = LogisticRegression()
    lg.fit(X_train, y_train)
    return lg.predict_proba(X_test)

def insert_into_database(df, table_name):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="Sk1ttles", db="nba_stats"))
    df.to_sql(con=engine, name=table_name, if_exists='append', index=False)

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
    try:
        myConnection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod', autocommit=True)
    except:
        print('Failed to connect to database')
        sys.exit(1)

    driver = '/Users/Philip/Downloads/chromedriver'
    current_date = str(datetime.date.today())
    team_list = get_games(driver)

    train_lin_reg_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[1])]).format(current_date))
    train_lin_reg_df.loc[:, 'minutes_played'] = train_lin_reg_df.loc[:, 'minutes_played'].apply(time_convert)
    train_lin_reg_df = concat_drop(train_lin_reg_df, ['home_away'], ['player_id', 'team', 'game_hash', 'game_date', 'home_away', 'fg', '3p', 'ft'])
    #X_train, X_test, y_train, y_test = train_split(train_lin_reg_df.loc[:, 'minutes_played':'defensive_rating'], train_lin_reg_df.loc[:, 'pts'])
    #gen_lin_reg_coef(X_train, X_test, y_train, y_test)

    train_log_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[2])]).format(current_date))
    train_log_df = concat_drop(train_log_df, ['home_away', 'win_lose'], ['home_away', 'win_lose'])

    lin_results, log_results = [], []
    for team in team_list:
        lin_query = ' '.join([i for i in extract_query(sys.argv[3])]).format(team, current_date, team, team)
        lin_results.append([lin_test(gen_df(myConnection, lin_query), train_lin_reg_df, team, current_date)])

        log_query = ' '.join([i for i in extract_query(sys.argv[4])]).format(team, current_date, team)
        log_results.append([log_test(gen_df(myConnection, log_query), train_log_df, team, current_date)])

    for i in range(0, len(lin_results), 2):
        print('R-Squared Value: {}'.format(lin_results[i][0][2]), '\t\t\t\t\t\t', 'R-Squared Value: {}'.format(lin_results[i+1][0][2]))
        print(pd.concat([lin_results[i][0][0], lin_results[i+1][0][0]], axis=1))
        print('Total Score: {}'.format(lin_results[i][0][1]), '\t\t\t\t\t', 'Total Score: {}'.format(lin_results[i+1][0][1]))
        print('Win Probability: {}'.format(log_results[i]), '\t\t\t', 'Win Probability: {}'.format(log_results[i+1]), '\n\n')\
