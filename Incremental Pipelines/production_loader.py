"""
spark-submit --jars /Users/Philip/Downloads/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar test_loader.py
"""

import os
import sys
import logging
import pymysql
import itertools
import json
import pandas as pd
import pyspark
from pyspark import SparkFiles
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

conf = SparkConf()
conf.set('spark.app.name', 'nba_stats_incrementals')
conf.set('spark.master', 'local[*]')
conf.set('spark.driver.cores', 8)
conf.set('spark.executor.cores', 1)
conf.set('spark.executor.instances', 2)
conf.set('spark.scheduler.mode', 'FIFO')
conf.set('spark.locality.wait', '0')
conf.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
conf.set('spark.debug.maxToStringFields', 100)

spark = SparkSession.builder.config(conf=conf).appName('Reload nba_stats_prod').getOrCreate()
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

def extract_json(file):
    with open(file) as f:
        return json.load(f)

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats', autocommit=True)

def sql_execute(sql):
    conn = gen_db_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    return results

def get_tables():
    sql = """select table_name
             from information_schema.tables
             where table_schema = 'nba_stats' and
                   table_type = 'BASE TABLE' and
                   table_name not in ('DATABASECHANGELOG', 'DATABASECHANGELOGLOCK')"""
    raw_tables = sql_execute(sql)
    tables = list(itertools.chain.from_iterable(raw_tables))
    return tables

def find_bounds(table, field):
    if field == 'season' and table in ['3pt_pct', 'fg_pct', 'pts', 'rebound_pct']:
        field = 'year'
    else:
        pass
    min = 'select min({field}) from {table}'.format(field=field, table=table)
    max = 'select max({field}) from {table}'.format(field=field, table=table)
    min_val = sql_execute(min)[0][0]
    max_val = sql_execute(max)[0][0]
    return min_val, max_val

def gen_sql(table, field):
    sql = table
    if table in ['basic_box_stats', 'advanced_box_stats', 'game_results']:
        field = 'game_date'
        sql = """(select m.game_date, b.*
                  from nba_stats.{table} as b
                  inner join nba_stats.box_score_map as m on b.game_hash = m.game_hash) as a""".format(table=table)
        min_val, max_val = find_bounds('box_score_map', 'game_date')
    else:
        min_val, max_val = find_bounds(table, field)
    return sql, field, min_val, max_val

def gen_df(table, url, partCol, lower, upper, user, password):
    print(table, '\n\n\n')
    df = sqlContext.read.format('jdbc').options(
             driver="com.mysql.jdbc.Driver",
             url=url,
             user=user,
             password=password,
             dbtable=table,
             fetchsize=10000,
             partitionColumn=partCol,
             lowerBound=lower,
             upperBound=upper,
             numPartitions=8
        ).load()
    #df.cache()
    return df

def gen_df_dict(nba_stats_tables, stats_jdbc_url, manifest_dict, user, password):
    replace_dict = {'traditional_team_boxscore_stats':'team_traditional_boxscore_stats',
                    'figure4_team_boxscore_stats':'team_figure4_boxscore_stats',
                    'advanced_team_boxscore_stats':'team_advanced_boxscore_stats',
                    'PTS':'points',
                    'injuries':'injured_players'}
    df_dict = {}
    for table in nba_stats_tables:
        lu_table = table
        if table in replace_dict:
            lu_table = replace_dict[table]
        else:
            pass
        partition_field = manifest_dict[lu_table.lower()]['partitionColumn']
        table, partition_field, min_val, max_val = gen_sql(table, partition_field)
        df = gen_df(table, stats_jdbc_url, partition_field, min_val, max_val, user, password)
        df_dict[lu_table.lower()] = df
    return df_dict

def write_to_db(df, jdbc_url, table, user, password):
    df.write.format('jdbc').options(
            driver="com.mysql.jdbc.Driver",
            url=jdbc_url,
            user=user,
            password=password,
            schema='nba_stats_prod',
            dbtable=table,
            batchsize=200
        ).mode('append').save()
    return

def gen_prod_player_info(player_info_df, table, url, user, password):
    prod_player_info = player_info_df.repartition(16, 'player_id')
    write_to_db(prod_player_info, url, table, user, password)
    prod_player_info = prod_player_info.persist()
    return prod_player_info

def gen_prod_team_info(team_info_df, table, user, url, password):
    write_to_db(team_info_df, url, table, user, password)
    team_info_df = team_info_df.persist()
    return team_info_df

def gen_prod_box_score_map(box_score_map_df, team_info_df, table, url, user, password):
    away_team_df = team_info_df.select(f.col('team_id').alias('away_team_id'),f.col('TEAM').alias('away_team'))
    home_team_df = team_info_df.select(f.col('team_id').alias('home_team_id'),f.col('TEAM').alias('home_team'))
    box_score_map_df = box_score_map_df.repartition(16, 'game_hash')
    prod_box_score_map = box_score_map_df.join(f.broadcast(away_team_df), box_score_map_df.away_team == away_team_df.away_team,'inner')\
                                         .join(f.broadcast(home_team_df), box_score_map_df.home_team == home_team_df.home_team, 'inner')\
                                         .select(f.col('game_hash'),\
                                                 f.col('away_team_id'),\
                                                 f.col('home_team_id'),\
                                                 f.col('game_date'),\
                                                 f.col('source_link'),\
                                                 f.col('created_at'))
    prod_box_score_map = prod_box_score_map.repartition(16, 'game_hash')
    prod_box_score_map = f.broadcast(prod_box_score_map)
    write_to_db(prod_box_score_map, url, table, user, password)
    return prod_box_score_map

def gen_prod_active_rosters(active_rosters, player_info, table, url, user, password):
    prod_active_rosters = active_rosters.alias('a').join(player_info.alias('p'), active_rosters.Name == player_info.name, 'left')\
                                                   .select(f.col('p.PLAYER_ID').alias('player_id'),\
                                                           f.col('a.name'),\
                                                           f.col('a.team_id'),\
                                                           f.col('a.team'),\
                                                           f.col('a.conference'),\
                                                           f.col('a.source_link'),\
                                                           f.col('a.created_at'))
    #prod_active_rosters = prod_active_rosters.repartition(8, 'player_id')
    write_to_db(prod_active_rosters, url, table, user, password)
    return

def gen_prod_injuries(injuries, table, url, user, password):
    write_to_db(injuries, url, table, user, password)
    return

def gen_prod_game_date_lu(game_date_lu, table, url, user, password):
    game_date_lu = game_date_lu.repartition(16, 'season')
    write_to_db(game_date_lu, url, table, user, password)
    return

def gen_prod_regularseasonaverages(reg_avg_df, team_info, player_info, table, url, user, password):
    reg_avg_df = reg_avg_df.repartition(16, 'SEASON')
    prod_reg_avg = reg_avg_df.alias('r').join(f.broadcast(team_info), reg_avg_df.TEAM == team_info.TEAM, 'inner')\
                                        .select(f.col('PLAYER_ID').alias('player_id'),\
                                                f.col('SEASON').alias('season'),\
                                                f.col('team_id'),\
                                                f.col('GP').alias('gp'),\
                                                f.col('GS').alias('gs'),\
                                                f.col('MIN').alias('min'),\
                                                f.when(f.col('fgm_a') == '',0).when(f.col('fgm_a').isNull(), 0)\
                                                        .otherwise(f.split(f.col('fgm_a'), '-').getItem(0)).alias('fg_m'),\
                                                f.when(f.col('fgm_a') == '',0).when(f.col('fgm_a').isNull(), 0)\
                                                        .otherwise(f.split(f.col('fgm_a'), '-').getItem(1)).alias('fg_a'),\
                                                f.col('fg_pct'),\
                                                f.when(f.col('3pm_a') == '',0).when(f.col('3pm_a').isNull(), 0)\
                                                        .otherwise(f.split(f.col('fgm_a'), '-').getItem(0)).alias('3p_m'),\
                                                f.when(f.col('3pm_a') == '',0).when(f.col('3pm_a').isNull(), 0)\
                                                        .otherwise(f.split(f.col('3pm_a'), '-').getItem(1)).alias('3p_a'),\
                                                f.col('3pt_pct').alias('3p_pct'),\
                                                f.when(f.col('ftm_a') == '',0).when(f.col('ftm_a').isNull(), 0)\
                                                        .otherwise(f.split(f.col('ftm_a'), '-').getItem(0)).alias('ft_m'),\
                                                f.when(f.col('ftm_a') == '',0).when(f.col('ftm_a').isNull(), 0)\
                                                        .otherwise(f.split(f.col('ftm_a'), '-').getItem(1)).alias('ft_a'),\
                                                f.col('ft_pct'),\
                                                f.col('or'),
                                                f.col('dr'),\
                                                f.col('reb'),\
                                                f.col('ast'),\
                                                f.col('blk'),\
                                                f.col('stl'),\
                                                f.col('pf'),\
                                                f.col('to'),\
                                                f.col('pts'),\
                                                f.col('r.created_at'))
    prod_reg_avg = prod_reg_avg.repartition(16, 'player_id')
    write_to_db(prod_reg_avg,  url, table, user, password)
    return

def gen_prod_regularseasontotals(reg_tots_df, team_info, player_info, table, url, user, password):
    reg_tots_df = reg_tots_df.repartition(16, 'SEASON')
    prod_reg_tots = reg_tots_df.alias('r').join(f.broadcast(team_info), reg_tots_df.TEAM == team_info.TEAM, 'inner')\
                                          .select(f.col('PLAYER_ID').alias('player_id'),\
                                                  f.col('SEASON').alias('season'),\
                                                  f.col('team_id'),\
                                                  f.when(f.col('fgm_a') == '',0).when(f.col('fgm_a').isNull(), 0)\
                                                          .otherwise(f.split(f.col('fgm_a'), '-').getItem(0)).alias('fg_m'),\
                                                  f.when(f.col('fgm_a') == '',0).when(f.col('fgm_a').isNull(), 0)\
                                                          .otherwise(f.split(f.col('fgm_a'), '-').getItem(1)).alias('fg_a'),\
                                                  f.col('fg_pct'),\
                                                  f.when(f.col('3pm_a') == '',0).when(f.col('3pm_a').isNull(), 0)\
                                                          .otherwise(f.split(f.col('fgm_a'), '-').getItem(0)).alias('3p_m'),\
                                                  f.when(f.col('3pm_a') == '',0).when(f.col('3pm_a').isNull(), 0)\
                                                          .otherwise(f.split(f.col('3pm_a'), '-').getItem(1)).alias('3p_a'),\
                                                  f.col('3pt_pct').alias('3p_pct'),\
                                                  f.when(f.col('ftm_a') == '',0).when(f.col('ftm_a').isNull(), 0)\
                                                          .otherwise(f.split(f.col('ftm_a'), '-').getItem(0)).alias('ft_m'),\
                                                  f.when(f.col('ftm_a') == '',0).when(f.col('ftm_a').isNull(), 0)\
                                                          .otherwise(f.split(f.col('ftm_a'), '-').getItem(1)).alias('ft_a'),\
                                                  f.col('ft_pct'),\
                                                  f.col('or'),
                                                  f.col('dr'),\
                                                  f.col('reb'),\
                                                  f.col('ast'),\
                                                  f.col('blk'),\
                                                  f.col('stl'),\
                                                  f.col('pf'),\
                                                  f.col('to'),\
                                                  f.col('pts'),\
                                                  f.col('r.created_at'))
    prod_reg_tots = prod_reg_tots.repartition(16, 'player_id')
    write_to_db(prod_reg_tots,  url, table, user, password)
    return

def gen_prod_regularseasonmisctotals(reg_misc_tots_df, team_info, player_info, table, url, user, password):
    reg_misc_tots_df = reg_misc_tots_df.repartition(16, 'SEASON')
    prod_reg_misc_tots = reg_misc_tots_df.alias('r').join(f.broadcast(team_info.alias('t')), reg_misc_tots_df.TEAM == team_info.TEAM, 'inner')\
                                                    .select(f.col('PLAYER_ID').alias('player_id'),\
                                                            f.col('SEASON').alias('season'),\
                                                            f.col('team_id'),\
                                                            f.col('DBLDBL').alias('dbldbl'),\
                                                            f.col('TRIDBL').alias('tridbl'),\
                                                            f.col('DQ').alias('dq'),\
                                                            f.col('EJECT').alias('eject'),\
                                                            f.col('TECH').alias('tech'),\
                                                            f.col('FLAG').alias('flag'),\
                                                            f.col('ast_to'),\
                                                            f.col('stl_to'),\
                                                            f.col('RAT').alias('rat'),\
                                                            f.col('SCEFF').alias('sceff'),\
                                                            f.col('SHEFF').alias('sheff'),\
                                                            f.col('r.created_at'))
    prod_reg_misc_tots = prod_reg_misc_tots.repartition(16, 'player_id')
    write_to_db(prod_reg_misc_tots,  url, table, user, password)
    return

def gen_prod_player_team_map(player_info, reg_avg_df, team_info, table, url, user, password):
    prod_player_team_map = player_info.alias('p').join(reg_avg_df.alias('r'), player_info.player_id == reg_avg_df.PLAYER_ID, 'inner')\
                                                 .join(f.broadcast(team_info.alias('t')), reg_avg_df.TEAM == team_info.TEAM, 'inner')\
                                                 .select(f.col('p.player_id'),\
                                                         f.col('r.SEASON').alias('season'),\
                                                         f.col('team_id')).distinct()
    prod_player_team_map = prod_player_team_map.repartition(16, 'player_id')
    write_to_db(prod_player_team_map, url, table, user, password)
    return

def gen_player_lu(player_info_df, player_team_map_df, game_date_lu_df):
    player_lu = player_info_df.alias('p').join(player_team_map_df.alias('map'), player_info_df.player_id == player_team_map_df.player_id, 'inner')\
                                  .join(f.broadcast(game_date_lu_df.alias('lu')), player_team_map_df.season == game_date_lu_df.season)\
                                  .select(f.col('p.player_id'),\
                                          f.col('name'),\
                                          f.col('map.team'),\
                                          f.col('lu.day').alias('game_date'),\
                                          f.col('lu.season'))
    return player_lu

def gen_prod_basic_box_stats(basic_df, box_score_map, player_lu, team_info, table, url, user, password):
    basic_df = basic_df.repartition(64, 'game_hash')
    prod_basic_df = basic_df.alias('b').join(box_score_map.alias('map'), basic_df.game_hash == box_score_map.game_hash, 'inner')\
                                       .join(f.broadcast(team_info.alias('t')), basic_df.team == team_info.TEAM, 'inner')\
                                       .join(player_lu.alias('p'), (basic_df.name == player_lu.name) & \
                                                                   (basic_df.team == player_lu.team) & \
                                                                   (box_score_map.game_date == player_lu.game_date), 'inner')\
                                       .select(f.col('map.game_hash'),\
                                               f.col('p.player_id'),\
                                               f.col('t.team_id'),\
                                               f.col('b.MP').alias('seconds_played'),\
                                               f.col('b.FG'),\
                                               f.col('b.FGA'),\
                                               f.col('b.FG_PCT'),\
                                               f.col('b.3P'),\
                                               f.col('b.3PA'),\
                                               f.col('b.3P_PCT'),\
                                               f.col('b.FT'),\
                                               f.col('b.FTA'),\
                                               f.col('b.FT_PCT'),\
                                               f.col('b.ORB'),\
                                               f.col('b.DRB'),\
                                               f.col('b.TRB'),\
                                               f.col('b.AST'),\
                                               f.col('b.STL'),\
                                               f.col('b.BLK'),\
                                               f.col('b.TOV'),\
                                               f.col('b.PF'),\
                                               f.col('b.PTS'),\
                                               f.col('b.PLUS_MINUS').alias('plus_minus'),\
                                               f.col('b.source_link'),\
                                               f.col('b.created_at'))
    prod_basic_df = prod_basic_df.repartition(224, 'game_hash')
    write_to_db(prod_basic_df,  url, table, user, password)
    return

def gen_prod_adv_box_stats(adv_stats_df, box_score_map, player_lu, team_info, table, url, user, password):
    adv_stats_df = adv_stats_df.repartition(64, 'game_hash')
    prod_adv_df = adv_stats_df.alias('a').join(box_score_map.alias('map'), adv_stats_df.game_hash == box_score_map.game_hash, 'inner')\
                                         .join(f.broadcast(team_info.alias('t')), adv_stats_df.team == team_info.TEAM, 'inner')\
                                         .join(player_lu.alias('p'), (adv_stats_df.name == player_lu.name) & \
                                                                     (adv_stats_df.team == player_lu.team) & \
                                                                     (box_score_map.game_date == player_lu.game_date), 'inner')\
                                         .select(f.col('map.game_hash'),\
                                                 f.col('p.player_id'),\
                                                 f.col('t.team_id'),\
                                                 f.col('a.MP').alias('seconds_played'),\
                                                 f.col('a.TS_PCT').alias('true_shooting_pct'),\
                                                 f.col('a.EFG_PCT').alias('effective_fg_pct'),\
                                                 f.col('a.3PAR').alias('3P_attempt_rate'),\
                                                 f.col('a.FTR').alias('FT_attempt_rate'),\
                                                 f.col('a.ORB_PCT').alias('offensive_reb_rate'),\
                                                 f.col('a.DRB_PCT').alias('defensive_reb_rate'),\
                                                 f.col('a.TRB_PCT').alias('total_reb_pct'),\
                                                 f.col('a.AST_PCT').alias('assist_pct'),\
                                                 f.col('a.STL_PCT').alias('steal_pct'),\
                                                 f.col('a.BLK_PCT').alias('block_pct'),\
                                                 f.col('a.TOV_PCT').alias('turnover_pct'),\
                                                 f.col('a.USG_PCT').alias('usage_pct'),\
                                                 f.col('a.ORTG').alias('offensive_rating'),\
                                                 f.col('a.DRTG').alias('defensive_rating'),\
                                                 f.col('a.source_link'),\
                                                 f.col('a.created_at'))
    prod_adv_df = prod_adv_df.repartition(72, 'game_hash')
    write_to_db(prod_adv_df,  url, table, user, password)
    return

def gen_game_lu(box_score_map):
    game_lu = box_score_map.select(f.col('game_hash'),\
                                   f.col('home_team').alias('team'),\
                                   f.col('game_date')).union(\
              box_score_map.select(f.col('game_hash'),\
                                   f.col('away_team').alias('team'),\
                                   f.col('game_date'))).distinct()
    return game_lu

def gen_prod_player_misc_stats(player_misc_df, game_lu, player_lu, team_info, table, url, user, password):
    player_misc_df = player_misc_df.repartition(48, 'team')
    prod_misc_df = player_misc_df.alias('m').join(player_lu.alias('p'), \
                                                 (player_misc_df.team == player_lu.team) & \
                                                 (player_misc_df.game_date == player_lu.game_date) & \
                                                 (player_misc_df.name == player_lu.name),\
                                                 'inner')\
                                            .join(game_lu.alias('map'),\
                                                 (player_misc_df.team == game_lu.team) & \
                                                 (player_misc_df.game_date == game_lu.game_date), \
                                                 'inner')\
                                            .join(team_info.alias('t'),\
                                                  player_misc_df.team == team_info.TEAM,\
                                                 'inner')\
                                            .select(f.col('map.game_hash'),\
                                                    f.col('p.player_id'),\
                                                    f.col('t.team_id'),\
                                                    f.col('m.PTS_OFF_TO').alias('pts_off_to'),\
                                                    f.col('m.2nd_PTS').alias('second_chance_pts'),\
                                                    f.col('m.FBPs').alias('fbps'),\
                                                    f.col('m.PITP').alias('pitp'),\
                                                    f.col('m.OppPTS_OFF_TO').alias('opp_pts_off_to'),\
                                                    f.col('m.Opp2nd_PTS').alias('opp_second_chance_pts'),\
                                                    f.col('m.OppFBPs').alias('opp_fbps'),\
                                                    f.col('m.OppPITP').alias('opp_pitp'),\
                                                    f.col('m.BLK').alias('blk'),\
                                                    f.col('m.BLKA').alias('blka'),\
                                                    f.col('m.PF').alias('pf'),\
                                                    f.col('m.PFD').alias('pfd'),\
                                                    f.col('m.source_link'),\
                                                    f.col('m.created_at'))
    prod_misc_df = prod_misc_df.repartition(72, 'game_hash')
    write_to_db(prod_misc_df, url, table, user, password)
    return

def gen_prod_player_scoring_stats(player_scoring_df, game_lu, player_lu, team_info, table, url, user, password):
    player_scoring_df = player_scoring_df.repartition(72, 'team')
    prod_scoring_df = player_scoring_df.alias('s').join(player_lu.alias('p'), \
                                                       (player_scoring_df.team == player_lu.team) & \
                                                       (player_scoring_df.game_date == player_lu.game_date) & \
                                                       (player_scoring_df.name == player_lu.name),\
                                                       'inner')\
                                                  .join(game_lu.alias('map'),\
                                                       (player_scoring_df.team == game_lu.team) & \
                                                       (player_scoring_df.game_date == game_lu.game_date), \
                                                       'inner')\
                                                  .join(team_info.alias('t'),\
                                                        player_scoring_df.team == team_info.TEAM,\
                                                       'inner')\
                                                  .select(f.col('map.game_hash'),\
                                                          f.col('p.player_id'),\
                                                          f.col('t.team_id'),\
                                                          (f.col('s.%FGA2PT')/100).alias('pct_2pt_fga'),\
                                                          (f.col('s.%FGA3PT')/100).alias('pct_3pt_fga'),\
                                                          (f.col('s.%PTS2PT')/100).alias('pct_pt_2pt'),\
                                                          (f.col('s.%PTS2PT MR')/100).alias('pct_pts_2pt_mr'),\
                                                          (f.col('s.%PTS3PT')/100).alias('pct_pts_3pt'),\
                                                          (f.col('s.%PTSFBPs')/100).alias('pct_pts_fbps'),\
                                                          (f.col('s.%PTSFT')/100).alias('pct_pts_ft'),\
                                                          (f.col('s.%PTSOffTO')/100).alias('pct_pts_off_to'),\
                                                          (f.col('s.%PTSPITP')/100).alias('pct_pts_pitp'),\
                                                          (f.col('s.2FGM%AST')/100).alias('2pt_fgm_pct_ast'),\
                                                          (f.col('s.2FGM%UAST')/100).alias('2pt_fgm_pct_uast'),\
                                                          (f.col('s.3FGM%AST')/100).alias('3pt_fgm_pct_ast'),\
                                                          (f.col('s.3FGM%UAST')/100).alias('3pt_fgm_pct_uast'),\
                                                          (f.col('s.FGM%AST')/100).alias('fgm_pct_ast'),\
                                                          (f.col('s.FGM%UAST')/100).alias('fgm_pct_uast'),\
                                                          f.col('s.source_link'),\
                                                          f.col('s.created_at'))
    prod_scoring_df = prod_scoring_df.repartition(96, 'game_hash')
    write_to_db(prod_scoring_df, url, table, user, password)
    return

def gen_prod_player_usage_stats(player_usage_df, game_lu, player_lu, team_info, table, url, user, password):
    player_usage_df = player_usage_df.repartition(72, 'team')
    prod_usage_df = player_usage_df.alias('u').join(player_lu.alias('p'), \
                                                   (player_usage_df.team == player_lu.team) & \
                                                   (player_usage_df.game_date == player_lu.game_date) & \
                                                   (player_usage_df.name == player_lu.name) ,\
                                                   'inner')\
                                               .join(game_lu.alias('map'),\
                                                   (player_usage_df.team == game_lu.team) & \
                                                   (player_usage_df.game_date == game_lu.game_date), \
                                                   'inner')\
                                               .join(team_info.alias('t'),\
                                                    player_usage_df.team == team_info.TEAM,\
                                                    'inner')\
                                               .select(f.col('map.game_hash'),\
                                                       f.col('p.player_id'),\
                                                       f.col('t.team_id'),\
                                                       (f.col('u.USG%')/100).alias('usage_pct'),\
                                                       (f.col('u.%FGM')/100).alias('pct_fgm'),\
                                                       (f.col('u.%FGA')/100).alias('pct_fga'),\
                                                       (f.col('u.%3PM')/100).alias('pct_3pm'),\
                                                       (f.col('u.%3PA')/100).alias('pct_3pa'),\
                                                       (f.col('u.%FTM')/100).alias('pct_ftm'),\
                                                       (f.col('u.%FTA')/100).alias('pct_fta'),\
                                                       (f.col('u.%OREB')/100).alias('pct_oreb'),\
                                                       (f.col('u.%DREB')/100).alias('pct_dreb'),\
                                                       (f.col('u.%REB')/100).alias('pct_reb'),\
                                                       (f.col('u.%AST')/100).alias('pct_ast'),\
                                                       (f.col('u.%TOV')/100).alias('pct_tov'),\
                                                       (f.col('u.%STL')/100).alias('pct_stl'),\
                                                       (f.col('u.%BLK')/100).alias('pct_blk'),\
                                                       (f.col('u.%BLKA')/100).alias('pct_blka'),\
                                                       (f.col('u.%PF')/100).alias('pct_pf'),\
                                                       (f.col('u.%PFD')/100).alias('pct_pfd'),\
                                                       (f.col('u.%PTS')/100).alias('pct_pts'),\
                                                       f.col('u.source_link'),\
                                                       f.col('u.created_at'))
    prod_usage_df = prod_usage_df.repartition(96, 'game_hash')
    write_to_db(prod_usage_df, url, table, user, password)
    return

def gen_prod_game_results(game_results_df, box_score_map, table, url, user, password):
    game_results_df = game_results_df.repartition(32, 'game_hash')
    prod_game_results = game_results_df.alias('r').join(box_score_map.alias('b'), \
                                                        game_results_df.game_hash == box_score_map.game_hash, \
                                                        'inner')\
                                                   .select(f.col('r.game_hash'),\
                                                           f.col('r.away_score'),\
                                                           f.col('r.home_score'),\
                                                           f.col('r.created_at'))
    write_to_db(prod_game_results, url, table, user, password)
    return

def gen_prod_team_standings(team_standings, team_info, table, url, user, password):
    team_standings = team_standings.repartition(16, 'season')
    prod_team_standings = team_standings.alias('stand').join(f.broadcast(team_info).alias('t'), \
                                                         team_standings.team == team_info.TEAM, \
                                                         'inner')\
                                                       .select(f.col('stand.team'),\
                                                               f.col('stand.conference'),
                                                               f.col('stand.season'),\
                                                               f.col('stand.W').alias('wins'),\
                                                               f.col('stand.L').alias('loses'),\
                                                               f.col('stand.PCT').alias('pct'),\
                                                               f.col('stand.GB').alias('gb'),\
                                                               f.split(f.col('stand.HOME'), '-').getItem(0).alias('home_wins'),\
                                                               f.split(f.col('stand.HOME'), '-').getItem(1).alias('home_loses'),\
                                                               f.split(f.col('stand.AWAY'), '-').getItem(0).alias('away_wins'),\
                                                               f.split(f.col('stand.AWAY'), '-').getItem(1).alias('away_loses'),\
                                                               f.split(f.col('stand.DIV'), '-').getItem(0).alias('div_wins'),\
                                                               f.split(f.col('stand.DIV'), '-').getItem(1).alias('div_loses'),\
                                                               f.split(f.col('stand.CONF'), '-').getItem(0).alias('conf_wins'),\
                                                               f.split(f.col('stand.CONF'), '-').getItem(1).alias('conf_loses'),\
                                                               f.col('stand.PPG').alias('ppg'),\
                                                               f.col('stand.OPP_PPG').alias('opp_ppg'),\
                                                               f.col('stand.DIFF').alias('diff'),\
                                                               f.col('stand.STRK').alias('strk'),\
                                                               f.col('stand.L10').alias('last_10'),\
                                                               f.col('stand.source_link'),\
                                                               f.col('stand.created_at'))
    write_to_db(prod_team_standings, url, table, user, password)
    return

def gen_prod_3pt_pct(df_3pt_pct, table, url, user, password):
    prod_3pt_pct = df_3pt_pct.select(f.col('team_id'),\
                                     f.col('season'),\
                                     f.col('OWN').alias('own_3pt_pct'),\
                                     f.col('OPP').alias('opp_3pt_pct'),\
                                     f.col('FT%').alias('ft_pct'),
                                     f.col('source_link'),\
                                     f.col('created_at'))
    prod_3pt_pct = prod_3pt_pct.repartition(16, 'season')
    write_to_db(prod_3pt_pct, url, table, user, password)
    return

def gen_prod_fg_pct(fg_pct_df, table, url, user, password):
    prod_fg_pct = fg_pct_df.select(f.col('team_id'),\
                                   f.col('season'),\
                                   f.col('OWN').alias('own_fg_pct'),\
                                   f.col('OPP').alias('opp_fg_pct'),\
                                   f.col('source_link'),\
                                   f.col('created_at'))
    prod_fg_pct = prod_fg_pct.repartition(16, 'season')
    write_to_db(prod_fg_pct, url, table, user, password)
    return

def gen_prod_pts(pts_df, table, url, user, password):
    prod_pts_df = pts_df.select(f.col('team_id'),\
                                f.col('season'),\
                                f.col('OWN').alias('own_pts'),\
                                f.col('OPP').alias('opp_pts'),\
                                f.col('DIFF').alias('diff'),\
                                f.col('source_link'),\
                                f.col('created_at'))
    prod_pts_df = prod_pts_df.repartition(16, 'season')
    write_to_db(prod_pts_df, url, table, user, password)
    return

def gen_prod_rebounds(rebounds_df, table, url, user, password):
    prod_rebounds_df = rebounds_df.select(f.col('team_id'),\
                                          f.col('season'),\
                                          f.col('OFF').alias('off_reb'),\
                                          f.col('DEF').alias('def_reb'),\
                                          f.col('TOT').alias('tot_reb'),\
                                          f.col('source_link'),\
                                          f.col('created_at'))
    prod_rebounds_df = prod_rebounds_df.repartition(16, 'season')
    write_to_db(prod_rebounds_df, url, table, user, password)
    return

def gen_prod_turnovers(turnovers_df, table, url, user, password):
    prod_turnovers_df = turnovers_df.select(f.col('team_id'),\
                                            f.col('season'),\
                                            f.col('OWN').alias('own_to'),\
                                            f.col('OPP').alias('opp_to'),\
                                            f.col('source_link'),\
                                            f.col('created_at'))
    prod_turnovers_df = prod_turnovers_df.repartition(16, 'season')
    write_to_db(prod_turnovers_df, url, table, user, password)
    return

def gen_prod_team_adv_stats(team_adv_stats, team_info, game_lu, table, url, user, password):
    prod_team_adv_stats = team_adv_stats.alias('a').join(game_lu.alias('map'), \
                                                        (team_adv_stats.TEAM == game_lu.team) &\
                                                        (team_adv_stats.GAME_DATE == game_lu.game_date),\
                                                        'inner')\
                                                   .join(f.broadcast(team_info).alias('t'), \
                                                         team_adv_stats.TEAM == team_info.TEAM)\
                                                   .select(f.col('map.game_hash'),\
                                                           f.col('t.team_id'),\
                                                           f.col('a.W/L').alias('win_lose'),\
                                                           f.col('a.MIN').alias('game_length'),\
                                                           f.col('a.OFFRTG').alias('off_rating'),\
                                                           f.col('a.DEFRTG').alias('def_rating'),\
                                                           f.col('a.NETRTG').alias('net_rating'),\
                                                           (f.col('a.AST%')/100).alias('ast_pct'),\
                                                           (f.col('a.AST/TO')/100).alias('ast_to_to'),\
                                                           f.col('a.AST_RATIO').alias('ast_ratio'),\
                                                           (f.col('a.OREB%')/100).alias('offensive_reb_pct'),\
                                                           (f.col('DREB%')/100).alias('defensive_reb_pct'),\
                                                           (f.col('a.REB%')/100).alias('reb_pct'),\
                                                           (f.col('a.TOV%')/100).alias('to_pct'),\
                                                           (f.col('a.EFG%')/100).alias('effective_fg_pct'),\
                                                           (f.col('a.TS%')/100).alias('ts_pct'),\
                                                           f.col('a.PACE').alias('pace'),\
                                                           f.col('a.PIE').alias('pie'),\
                                                           f.col('a.source_link'),\
                                                           f.col('a.created_at'))
    prod_team_adv_stats = prod_team_adv_stats.repartition(24, 'game_hash')
    write_to_db(prod_team_adv_stats, url, table, user, password)
    return

def gen_prod_team_fig4_stats(team_fig4_stats, team_info, game_lu, table, url, user, password):
    prod_team_fig4_stats = team_fig4_stats.alias('a').join(game_lu.alias('map'), \
                                                         (team_fig4_stats.TEAM == game_lu.team) &\
                                                         (team_fig4_stats.GAME_DATE == game_lu.game_date),\
                                                         'inner')\
                                                    .join(f.broadcast(team_info).alias('t'), \
                                                          team_fig4_stats.TEAM == team_info.TEAM)\
                                                    .select(f.col('map.game_hash'),\
                                                            f.col('t.team_id'),\
                                                            f.col('a.W/L').alias('win_lose'),\
                                                            f.col('a.MIN').alias('game_length'),\
                                                            (f.split(f.col('a.EFG%'), '%').getItem(0)/100).alias('effective_fg_pct'),\
                                                            f.col('a.FTA_RATE').alias('fta_rate'),\
                                                            (f.col('a.TOV%')/100).alias('tov_pct'),\
                                                            (f.split(f.col('a.OREB%'), '%').getItem(0)/100).alias('oreb_pct'),\
                                                            (f.split(f.col('a.OPP_EFG%'), '%').getItem(0)/100).alias('opp_effective_fg_pct'),\
                                                            f.col('a.OPP_FTA_RATE').alias('opp_fta_rate'),\
                                                            (f.col('OPP_TOV%')/100).alias('opp_tov_pct'),\
                                                            (f.split(f.col('a.OPP_OREB%'), '%').getItem(0)/100).alias('opp_off_reb_pct'),\
                                                            f.col('a.source_link'),\
                                                            f.col('a.created_at'))
    prod_team_fig4_stats = prod_team_fig4_stats.repartition(24, 'game_hash')
    write_to_db(prod_team_fig4_stats, url, table, user, password)
    return

def gen_prod_team_trad_stats(team_trad_stats, team_info, game_lu, table, url, user, password):
    prod_team_trad_stats = team_trad_stats.alias('a').join(game_lu.alias('map'), \
                                                         (team_trad_stats.TEAM == game_lu.team) &\
                                                         (team_trad_stats.GAME_DATE == game_lu.game_date),\
                                                         'inner')\
                                                    .join(f.broadcast(team_info).alias('t'), \
                                                          team_trad_stats.TEAM == team_info.TEAM)\
                                                    .select(f.col('map.game_hash'),\
                                                            f.col('t.team_id'),\
                                                            f.col('a.W/L').alias('win_lose'),\
                                                            f.col('a.MIN').alias('game_length'),\
                                                            f.col('a.PTS').alias('pts'),\
                                                            f.col('a.FGM').alias('fgm'),\
                                                            f.col('a.FGA').alias('fga'),\
                                                            (f.col('a.FG%')/100).alias('fg_pct'),\
                                                            f.col('a.3PM').alias('3pm'),\
                                                            f.col('a.3PA').alias('3pa'),\
                                                            (f.col('a.3P%')/100).alias('3p_pct'),\
                                                            f.col('a.FTM').alias('ftm'),\
                                                            f.col('a.FTA').alias('fta'),\
                                                            (f.col('FT%')/100).alias('ft_pct'),\
                                                            f.col('a.OREB').alias('oreb'),\
                                                            f.col('a.DREB').alias('dreb'),\
                                                            f.col('a.REB').alias('tot_reb'),\
                                                            f.col('a.AST').alias('ast'),\
                                                            f.col('a.TOV').alias('tov'),\
                                                            f.col('a.STL').alias('stl'),\
                                                            f.col('a.BLK').alias('blk'),\
                                                            f.col('a.PF').alias('personal_fouls'),\
                                                            f.col('a.+/-').alias('plus_minus'),\
                                                            f.col('a.source_link'),\
                                                            f.col('a.created_at'))
    prod_team_trad_stats = prod_team_trad_stats.repartition(24, 'game_hash')
    write_to_db(prod_team_trad_stats, url, table, user, password)
    return

def gen_prod_team_scoring_stats(team_scoring_stats, team_info, game_lu, table, url, user, password):
    prod_team_scoring_stats = team_scoring_stats.alias('a').join(game_lu.alias('map'), \
                                                                (team_scoring_stats.TEAM == game_lu.team) &\
                                                                (team_scoring_stats.GAME_DATE == game_lu.game_date),\
                                                                'inner')\
                                                           .join(f.broadcast(team_info).alias('t'), \
                                                                 team_scoring_stats.TEAM == team_info.TEAM)\
                                                           .select(f.col('map.game_hash'),\
                                                                   f.col('t.team_id'),\
                                                                   f.col('a.W/L').alias('win_lose'),\
                                                                   f.col('a.MIN').alias('game_length'),\
                                                                   (f.col('a.%FGA_2PT')/100).alias('pct_2pt_fg'),\
                                                                   (f.col('a.%FGA_3PT')/100).alias('pct_3pt_fg'),\
                                                                   (f.col('%PTS_2PT')/100).alias('pct_pts_2pt'),\
                                                                   (f.col('%PTS_2PT_MR')/100).alias('pct_pts_2pt_mr'),\
                                                                   (f.col('%PTS_3PT')/100).alias('pct_pts_3pt'),\
                                                                   (f.col('%PTS_FBPS')/100).alias('pct_pts_fbps'),\
                                                                   (f.col('%PTS_FT')/100).alias('pct_pts_ft'),\
                                                                   (f.col('%PTS_OFF_TO')/100).alias('pct_pts_off_to'),\
                                                                   (f.col('%PTS_PITP')/100).alias('pct_pts_pitp'),\
                                                                   (f.col('2FGM_%AST')/100).alias('2pt_fgm_ast_pct'),\
                                                                   (f.col('2FGM_%UAST')/100).alias('2pt_fgm_uast_pct'),\
                                                                   (f.col('3FGM_%AST')/100).alias('3pt_fgm_ast_pct'),\
                                                                   (f.col('3FGM_%UAST')/100).alias('3pt_fgm_uast_pct'),\
                                                                   (f.col('FGM_%AST')/100).alias('fgm_pct_ast'),\
                                                                   (f.col('FGM_%UAST')/100).alias('fgm_pct_uast'),\
                                                                   f.col('a.source_link'),\
                                                                   f.col('a.created_at'))
    prod_team_scoring_stats = prod_team_scoring_stats.repartition(24, 'game_hash')
    write_to_db(prod_team_scoring_stats, url, table, user, password)
    return

def gen_prod_team_misc_stats(team_misc_stats, team_info, game_lu, table, url, user, password):
    prod_team_misc_stats = team_misc_stats.alias('a').join(game_lu.alias('map'), \
                                                          (team_misc_stats.TEAM == game_lu.team) &\
                                                          (team_misc_stats.GAME_DATE == game_lu.game_date),\
                                                          'inner')\
                                                     .join(f.broadcast(team_info).alias('t'), \
                                                           team_misc_stats.TEAM == team_info.TEAM)\
                                                     .select(f.col('map.game_hash'),\
                                                             f.col('t.team_id'),\
                                                             f.col('a.W/L').alias('win_lose'),\
                                                             f.col('a.MIN').alias('game_length'),\
                                                             f.col('a.PTS_OFF_TO').alias('points_off_to'),\
                                                             f.col('a.2ND_PTS').alias('second_chance_pts'),\
                                                             f.col('a.FBPS').alias('fbps'),\
                                                             f.col('a.PITP').alias('pts_in_paint'),\
                                                             f.col('a.OPP_PTS_OFF_TO').alias('opp_pts_off_to'),\
                                                             f.col('a.OPP_2ND_PTS').alias('opp_second_chance_pts'),\
                                                             f.col('a.OPP_FBPS').alias('opp_fbps'),\
                                                             f.col('a.OPP_PITP').alias('opp_pts_in_paint'),\
                                                             f.col('a.source_link'),\
                                                             f.col('a.created_at'))
    prod_team_misc_stats = prod_team_misc_stats.repartition(24, 'game_hash')
    write_to_db(prod_team_misc_stats, url, table, user, password)
    return

def gen_prod_c_values(c_values, table, url, user, password):
    write_to_db(c_values, url, table, user, password)
    return

def gen_prod_lasso_alphas(lasso_alphas, table, url, user, password):
    write_to_db(lasso_alphas, url, table, user, password)
    return

def gen_prod_tot_pts_pred(tot_pts_preds, table, url, user, password):
    write_to_db(tot_pts_preds, url, table, user, password)
    return

def gen_prod_player_preds(player_preds, table, url, user, password):
    write_to_db(player_preds, url, table, user, password)
    return

def gen_prod_win_prod(win_prob, table, url, user, password):
    write_to_db(win_prob, url, table, user, password)
    return

def gen_prod_auditlog(auditlog, table, url, user, password):
    write_to_db(auditlog, url, table, user, password)
    return

if __name__ == '__main__':
    manifest_json = 'manifest_json.json'
    user = 'root'
    password = 'Sk1ttles'
    out_schema = 'nba_stats_prod'
    stats_jdbc_url = "jdbc:mysql://localhost:3306/nba_stats?autoReconnect=true&useSSL=false&rewriteBatchedStatements=true"
    prod_jdbc_url = "jdbc:mysql://localhost:3306/nba_stats_prod?autoReconnect=true&useSSL=false&rewriteBatchedStatements=true"
    tables = get_tables()
    manifest_dict = extract_json(manifest_json)
    df_dict = gen_df_dict(tables, stats_jdbc_url, manifest_dict, user, password)
    config_kwarg = dict(url=prod_jdbc_url, user=user, password=password)

    player_info = gen_prod_player_info(df_dict['player_info'], 'player_info', **config_kwarg)
    team_info = gen_prod_team_info(df_dict['team_info'], 'team_info', **config_kwarg)
    box_score_map = gen_prod_box_score_map(df_dict['box_score_map'], df_dict['team_info'], 'box_score_map', **config_kwarg)
    gen_prod_active_rosters(df_dict['active_rosters'], df_dict['player_info'], 'active_rosters', **config_kwarg)
    gen_prod_injuries(df_dict['injured_players'], 'injured_players', **config_kwarg)
    gen_prod_game_date_lu(df_dict['game_date_lookup'], 'game_date_lookup', **config_kwarg)
    gen_prod_regularseasonaverages(df_dict['regularseasonaverages'], \
                                   team_info, \
                                   player_info, \
                                   'regularseasonaverages', **config_kwarg)
    gen_prod_regularseasontotals(df_dict['regularseasontotals'], \
                                 team_info, \
                                 player_info, \
                                 'regularseasontotals', **config_kwarg)
    gen_prod_regularseasonmisctotals(df_dict['regularseasonmisctotals'], \
                                     team_info, \
                                     player_info, \
                                     'regularseasonmisctotals', **config_kwarg)
    gen_prod_player_team_map(player_info, \
                             df_dict['regularseasonaverages'], \
                             team_info, \
                             'player_team_map', **config_kwarg)
    player_lu = gen_player_lu(player_info, df_dict['player_team_map'], df_dict['game_date_lookup'])
    gen_prod_basic_box_stats(df_dict['basic_box_stats'], \
                             df_dict['box_score_map'], \
                             player_lu, \
                             team_info, \
                             'basic_box_stats', **config_kwarg)
    gen_prod_adv_box_stats(df_dict['advanced_box_stats'], \
                           df_dict['box_score_map'], \
                           player_lu, \
                           team_info, \
                           'advanced_box_stats', **config_kwarg)
    game_lu = gen_game_lu(df_dict['box_score_map'])
    gen_prod_player_misc_stats(df_dict['player_misc_stats'], \
                               game_lu, \
                               player_lu, \
                               team_info, \
                               'player_misc_stats', **config_kwarg)
    gen_prod_player_scoring_stats(df_dict['player_scoring_stats'], \
                                  game_lu, \
                                  player_lu, \
                                  team_info, \
                                  'player_scoring_stats', **config_kwarg)
    gen_prod_player_usage_stats(df_dict['player_usage_stats'], \
                                game_lu, \
                                player_lu, \
                                team_info, \
                                'player_usage_stats', **config_kwarg)
    gen_prod_game_results(df_dict['game_results'], \
                          box_score_map, \
                          'game_results', **config_kwarg)
    gen_prod_team_standings(df_dict['team_standings'], team_info, 'team_standings', **config_kwarg)
    gen_prod_3pt_pct(df_dict['3pt_pct'], '3pt_pct', **config_kwarg)
    gen_prod_fg_pct(df_dict['fg_pct'], 'fg_pct', **config_kwarg)
    gen_prod_pts(df_dict['points'], 'points', **config_kwarg)
    gen_prod_rebounds(df_dict['rebound_pct'], 'rebound_pct', **config_kwarg)
    gen_prod_turnovers(df_dict['turnovers'], 'turnovers', **config_kwarg)
    gen_prod_team_adv_stats(df_dict['team_advanced_boxscore_stats'], \
                            team_info, \
                            game_lu, \
                            'team_advanced_boxscore_stats', **config_kwarg)
    gen_prod_team_fig4_stats(df_dict['team_figure4_boxscore_stats'], \
                             team_info, \
                             game_lu, \
                             'team_figure4_boxscore_stats', **config_kwarg)
    gen_prod_team_trad_stats(df_dict['team_traditional_boxscore_stats'], \
                             team_info, \
                             game_lu, \
                             'team_traditional_boxscore_stats', **config_kwarg)
    gen_prod_team_scoring_stats(df_dict['team_scoring_boxscore_stats'], \
                                team_info, \
                                game_lu, \
                                'team_scoring_boxscore_stats', **config_kwarg)
    gen_prod_team_misc_stats(df_dict['team_misc_boxscore_stats'], \
                             team_info, \
                             game_lu, \
                             'team_misc_boxscore_stats', **config_kwarg)
    gen_prod_c_values(df_dict['c_values'], 'c_values', **config_kwarg)
    gen_prod_lasso_alphas(df_dict['lasso_alphas'], 'lasso_alphas', **config_kwarg)
    gen_prod_tot_pts_pred(df_dict['total_points_predictions'], 'total_points_predictions', **config_kwarg)
    gen_prod_player_preds(df_dict['player_prediction_results'], 'player_prediction_results', **config_kwarg)
    gen_prod_win_prod(df_dict['win_probability_results'], 'win_probability_results', **config_kwarg)
    gen_prod_auditlog(df_dict['pipeline_auditlog'], 'pipeline_auditlog', **config_kwarg)
