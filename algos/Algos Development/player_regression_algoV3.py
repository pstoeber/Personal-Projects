"""
Regression algorithm used to predict the number of points each player on a given team will score
against a specific opponent.  Data pulled from nba_stats_prod mysql database instance.

python3 player_regression_algoV3.py get_player_names.sql train_lin_test.sql test_lin_test_game.sql train_log_query.sql test_log_query.sql

"""

import numpy as np
import pandas as pd
import pymysql
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression

def get_games(chromeDriver, games_list, active_list):
    #current_date = str(datetime.date.today()).split('-')  ## needed to get games listed for a current date
    #link = "https://www.basketball-reference.com/boxscores/?month={}&day={}&year={}".format(current_date[1], current_date[2], current_date[0]) #spliced link
    link = "https://www.basketball-reference.com/boxscores/?month=3&day=13&year=2018"
    soup = BeautifulSoup(requests.get(link).content, "html.parser")
    for games in soup.findAll('table', {'class':'teams'}):
        for game in games.find_all('a', href=True):
            if game.text.strip() != 'Final':
                games_list.append(check_city(game.text))
                active_list.append(get_roster(game['href'], chromeDriver))
    return games_list, active_list

def check_city(city_team_string):
    city_swap_dict = {'LA Lakers':'Los Angeles Lakers'}
    if city_team_string in city_swap_dict:
        return city_swap_dict[city_team_string]
    else:
        return city_team_string

def get_roster(link, chromeDriver):
    translation_dict = {'uta':'utah', 'nop':'no', 'brk':'bkn', 'pho':'phx', 'cho':'cha'}
    city = link.split('/')[2].lower()
    if city in translation_dict:
        city = translation_dict[city]
    link = 'http://www.espn.com/nba/team/roster/_/name/{}/'.format(city)
    browser = webdriver.Chrome(executable_path=chromeDriver)
    browser.get(link)
    body = browser.find_element_by_xpath('//*[@id="fittPageContainer"]/div[3]/div[2]/div[1]/div/section/section/div[4]/section/table')

    roster_list = []
    for i in body.text.split('\n')[1:]:
        name = []
        for p in i.split():
            if p not in ['PG', 'SG', 'SF', 'PF', 'C', 'G', 'F']:
                name.append(p)
            else:
                break
        roster_list.append(' '.join([i for i in name[1:]]))
    browser.quit()
    return roster_list

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

    chromeDriver = '/Users/Philip/Downloads/chromedriver'

    test_roster = ['Aron Baynes',
                   'Justin Bibbs',
                   'Jabari Bird',
                   'Jaylen Brown',
                   'PJ Dozier',
                   'Marcus Georges-Hunt',
                   'Gordon Hayward',
                   'Al Horford',
                   'Kyrie Irving',
                   'Nick King',
                   'Walt Lemon Jr.',
                   'Marcus Morris',
                   'Semi Ojeleye',
                   'Terry Rozier',
                   'Marcus Smart',
                   'Jayson Tatum',
                   'Daniel Theis',
                   'Brad Wanamaker',
                   'Robert Williams',
                   'Guerschon Yabusele']

    team_list, active_list = get_games(chromeDriver, [], [])
    roster_list, total_points_df, total_points_list, r_list, prob = [], [], [], [], []

    for team, player_list in zip(team_list, active_list):
        players = '", "'.join([i for i in player_list])
        roster_list.append(sql_execute(myConnection, ' '.join([i for i in extract_query(sys.argv[1])]).format(players), []))

    train_lin_reg_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[2])]))
    train_lin_reg_df.loc[:, 'minutes_played'] = train_lin_reg_df.loc[:, 'minutes_played'].apply(time_convert)
    train_lin_reg_df = concat_drop(train_lin_reg_df, ['home_away'], ['player_id', 'team', 'game_hash', 'game_date', 'home_away', 'fg', '3p', 'ft'])
    X_train, X_test, y_train, y_test = train_split(train_lin_reg_df.loc[:, 'minutes_played':'defensive_rating'], train_lin_reg_df.loc[:, 'pts'])
    gen_lin_reg_coef(X_train, X_test, y_train, y_test)

    train_log_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[4])]))
    train_log_df = concat_drop(train_log_df, ['home_away', 'win_lose'], ['home_away', 'win_lose'])

    for team, roster in zip(team_list, roster_list):
        roster = '\', \''.join([i for i in roster])
        test_lin_reg_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[3])]).format(team, roster, team, team))
        test_lin_reg_df.loc[:, 'minutes_played'] = test_lin_reg_df.loc[:, 'minutes_played'].apply(time_convert)
        test_lin_reg_df = concat_drop(test_lin_reg_df, ['home_away'], ['home_away', 'fg', '3p', 'ft'])

        lin_input_coef, lin_intercept, r_square = gen_lin_reg_coef(train_lin_reg_df.loc[:, 'minutes_played':'defensive_rating'], \
                                                                   test_lin_reg_df.loc[:, 'minutes_played':'defensive_rating'], \
                                                                   train_lin_reg_df.loc[:, 'pts'], \
                                                                   test_lin_reg_df.loc[:, 'pts'])

        total_points = aggregrate_total_points(test_lin_reg_df, ['player_id', 'name', 'team'], 'minutes_played', 'defensive_rating', \
                                               lin_input_coef.T, lin_intercept.item(), 'pts')

        total_points_df.append(total_points)
        total_points_list.append(total_points.iloc[:, -1].sum())
        r_list.append(r_square)

        test_log_df = gen_df(myConnection, ' '.join([i for i in extract_query(sys.argv[5])]).format(team, team))
        test_log_df = concat_drop(test_log_df, ['home_away', 'win_lose'], ['home_away', 'win_lose']).mean().to_frame().T
        win_prob = gen_log_coef(train_log_df.drop('W', axis=1), test_log_df.drop('W', axis=1), \
                                             train_log_df.loc[:, 'W'], test_log_df.loc[:, 'W'])
        prob.append(win_prob)

    for i in range(0, len(total_points_df), 2):
        print('R-Squared Value: {}'.format(r_list[i]), '\t\t\t\t\t\t', 'R-Squared Value: {}'.format(r_list[i+1]))
        print(pd.concat([total_points_df[i], total_points_df[i+1]], axis=1))
        print('Total Score: {}'.format(total_points_list[i]), '\t\t\t\t\t', 'Total Score: {}'.format(total_points_list[i+1]))
        print('Win Probability: {}'.format(prob[i]), '\t\t\t', 'Win Probability: {}'.format(prob[i+1]), '\n\n')


### Player Names to update ###
# - All Names with Jr, or a . in them on the basketball reference side
# - T Waller-Prince Atlanta
# - Tim Hardawy Jr. Knicks
