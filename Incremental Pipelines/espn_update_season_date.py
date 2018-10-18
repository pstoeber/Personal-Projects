"""
script designed to update the season field within regularseason tables
changes from '17-'18 to 2018 to make querying and joining easier
"""

import pymysql
import logging
import datetime

def find_distinct_seasons(connection):

    select_dates = 'select distinct season from RegularSeasonAverages \
                    union distinct \
                    select distinct season from RegularSeasonTotals \
                    union distinct \
                    select distinct season from RegularSeasonMiscTotals'
    return sql_execute(connection, select_dates)

def sql_execute(connection, query):

    exe = connection.cursor()
    exe.execute(query)
    return exe.fetchall()

def parse_dates(dates):

    date_dict = {}
    for date in dates:
        if int(date[0][5:7]) >= 0 and int(date[0][5:7]) <= 20:
            date_dict[date[0]] = '20' + str(date[0][5:7])
        else:
            date_dict[date[0]] = '19' + str(date[0][5:7])
    return date_dict

def generate_update_statements(connection, date_dict, table_list):

    exe = connection.cursor()
    for table in table_list:
        for date in date_dict:
            exe.execute("update " + table + " set season = " + str(date_dict[date]) + " where season = '\\" + str(date[:4]) + "\\" + str(date[4:]) + "'")

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    myConnection = pymysql.connect(host="localhost", user="root", password="Sk1ttles", db="nba_stats_staging", autocommit=True)
    table_list = ['regularseasonaverages', 'RegularSeasonTotals', 'RegularSeasonMiscTotals']
    logging.info('Updating ESPN seasons {}'.format(str(datetime.datetime.now())))
    dates = find_distinct_seasons(myConnection)
    date_dict = parse_dates(dates)
    generate_update_statements(myConnection, date_dict, table_list)
    logging.info('ESPN seasons update completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
