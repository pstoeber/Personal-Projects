"""
python3 prediction_visualizations.py sql/extract_player_pred.sql sql/extract_win_lose_prob.sql sql/extract_total_pts_pred.sql sql/extract_total_pts_comp.sql
"""

import numpy as np
import pandas as pd
import pymysql
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import sys

def extract_sql(file_path):
    with open(file_path, 'r') as infile:
        return [i for i in infile.readlines()]

def create_cmd_str(query):
    return ' '.join([i for i in query])

def extract_data(conn, sql):
    return pd.read_sql(sql=sql, con=conn, index_col=None)

def player_pred_to_actual_pts(df):
    for group, data in df.groupby(['team', 'game_date']):
        plt.title('{team} Player Predictions'.format(team=group))
        sns.set_style('whitegrid')
        plt.title('Predicted Points Vs. Actual Points')
        plt.xticks(rotation=90)
        sns.barplot(x='name', y='pts', data=data, hue='flag', palette='plasma_r')
        plt.show()
    return

def win_lose_prob(df):
    plt.title('Win Probability Vs. Lose Probability')
    labels = 'lose_prob', 'win_prob'
    colors = ['red', 'green']
    explode = (.2, 0)
    plt.pie(df.iloc[0], explode=explode, labels=labels, colors=colors, autopct='%1.11f%%', shadow=True, startangle=140)
    plt.show()
    return

def plot_total_pts_pred(df):
    plt.title('Predicted Total Points Vs Actual Total Points')
    sns.lineplot(x='game_date', y='predicted_total_pts', data=df, color='red', legend='brief')
    sns.lineplot(x='game_date', y='r_squared', data=df, color='blue')
    plt.show()
    return

def predicted_total_pts_comparison(df):
    sns.set_style('whitegrid')
    plt.title('Predicted Total Points Vs. Actual Total Points')
    plt.xticks(rotation=90)
    #df.groupby(['team', 'game_date'])
    sns.barplot(x='team', y='pts', data=df, hue='flag', palette='plasma_r')
    plt.show()
    return

if __name__ == '__main__':
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod')
    player_pred_df = extract_data(connection, create_cmd_str(extract_sql(sys.argv[1])))
    player_pred_to_actual_pts(player_pred_df)
    win_lose_prob_df = extract_data(connection, create_cmd_str(extract_sql(sys.argv[2])))
    win_lose_prob(win_lose_prob_df)
    total_pts_pred_df = extract_data(connection, create_cmd_str(extract_sql(sys.argv[3])))
    plot_total_pts_pred(total_pts_pred_df)
    total_pts_comp_df = extract_data(connection, create_cmd_str(extract_sql(sys.argv[4])))
    predicted_total_pts_comparison(total_pts_comp_df)
