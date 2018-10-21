import datetime
import pymysql
import logging

def create_update_statements(conn):
    team_dict = {'Atlanta':'Atlanta Hawks',
                 'Brooklyn':'Brooklyn Nets',
                 'Charlotte':'Charlotte Hornets',
                 'Chicago':'Chicago Bulls',
                 'Cleveland':'Cleveland Cavaliers',
                 'Dallas':'Dallas Mavericks',
                 'Denver':'Denver Nuggets',
                 'Detroit':'Detroit Pistons',
                 'Indiana':'Indiana Pacers',
                 'Golden State':'Golden State Warriors',
                 'Houston':'Houston Rockets',
                 'LA Clippers':'LA Clippers',
                 'Los Angeles Lakers':'Los Angeles Lakers',
                 'Memphis':'Memphis Grizzlies',
                 'Miami':'Miami Heat',
                 'Milwaukee':'Milwaukee Bucks',
                 'Minnesota':'Minnesota Timberwolves',
                 'New Orleans':'New Orleans Pelicans',
                 'New York':'New York Knicks',
                 'Oklahoma City':'Oklahoma City Thunder',
                 'Orlando':'Orlando Magic',
                 'Philadelphia':'Philadelphia 76ers',
                 'Phoenix':'Phoenix Suns',
                 'Portland Trail':'Portland Trail Blazers',
                 'Sacramento':'Sacramento Kings',
                 'San Antonio':'San Antonio Spurs',
                 'Toronto':'Toronto Raptors',
                 'Utah':'Utah Jazz',
                 'Washington':'Washington Wizards'}

    table_list = ['total_points_predictions', 'win_probability_results']
    for table in table_list:
        for k, v in team_dict.items():
            update = 'update {} set team = \'{}\' where team = \'{}\''.format(table, v, k)
            sql_execute(conn, update)

def sql_execute(conn, update):
    exe = conn.cursor()
    exe.execute(update)

def main():
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_backup', autocommit=True)
    create_update_statements(connection)

if __name__ == '__main__':
    main()
