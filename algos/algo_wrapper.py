"""
command line arguement:

python3 algo_wrapper.py "/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/sql ddl/active_rosters_player_id.sql" "/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/sql ddl/active_rosters_team_info.sql" "/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/production_insert_statements/primary_queries" "/Users/Philip/Documents/NBA prediction script/Incremental Pipelines/production_insert_statements/multithread" "/Users/Philip/Documents/NBA prediction script/algos/Lasso/home_train.sql" "/Users/Philip/Documents/NBA prediction script/algos/Lasso/away_train.sql" "/Users/Philip/Documents/NBA prediction script/algos/Lasso/lasso_test.sql" "/Users/Philip/Documents/NBA prediction script/algos/Logistic/home_train.sql" "/Users/Philip/Documents/NBA prediction script/algos/Logistic/away_train.sql" "/Users/Philip/Documents/NBA prediction script/algos/Logistic/logistic_test.sql"
"""

import pymysql
import datetime
import hashlib
import os
import sys
sys.path.append('/Users/Philip/Documents/NBA prediction script/Incremental Pipelines')
from active_roster import main as active
sys.path.append('/Users/Philip/Documents/NBA prediction script/Incremental Pipelines')
from injured_players import main as injured
sys.path.append('/Users/Philip/Documents/NBA prediction script/Incremental Pipelines')
from migrate_to_prod_mp import main as load
from Lasso.lasso_players_model import main as lasso
from Logistic.logistic_regression_team_model import main as log

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_prod', autocommit=True)

def sql_execute(conn, sql):
    exe = conn.cursor()
    exe.execute(sql)
    return

def refresh_prod(conn):
    sql_execute(conn, 'drop database nba_stats_prod')
    sql_execute(conn, 'create database nba_stats_prod')
    return

def liquibase_call(file):
    os.system("""/usr/local/bin/liquibase --driver=com.mysql.jdbc.Driver \
                 --classpath="/Users/Philip/Downloads/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46-bin.jar" \
                 --changeLogFile={file} \
                 --url="jdbc:mysql://localhost:3306/nba_stats_prod?autoReconnect=true&amp;useSSL=false" \
                 --username=root \
                 --password=Sk1ttles \
                 update""".format(file=file))
    return

def gen_hash(row):
    return  hashlib.md5(row.encode('utf-8')).hexdigest()

def pipeline_auditlog(conn, desc):
    pipeline_insert = 'insert into nba_stats.pipeline_auditlog values ("{}", "{}", "{}")'.format(gen_hash(str(datetime.datetime.now())), str(datetime.datetime.now()), desc)
    sql_execute(conn, pipeline_insert)
    return

if __name__ == '__main__':
    desc = 'algo refresh pipeline run'
    conn = gen_db_conn()
    active(sys.argv[1], sys.argv[2])
    injured(sys.argv[1])
    refresh_prod(conn)
    liquibase_call('/Users/Philip/Documents/NBA\ prediction\ script/Changelogs/nba_stats_prod_changeLogProd.xml')
    load(sys.argv[3], sys.argv[4])
    liquibase_call('/Users/Philip/Documents/NBA\ prediction\ script/Changelogs/nba_stats_prod_changeLogKeys.xml')
    pipeline_auditlog(conn, desc)
    lasso(sys.argv[5], sys.argv[6], sys.argv[7])
    log(sys.argv[8], sys.argv[9], sys.argv[10])
