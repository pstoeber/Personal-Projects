insert into nba_stats_prod.advanced_box_stats(
  select a.game_hash,
       p.player_id,
       p.team,
       a.MP,
       a.TS_PCT,
       a.EFG_PCT,
       a.3PAR,
       a.FTR,
       a.ORB_PCT,
       a.DRB_PCT,
       a.TRB_PCT,
       a.AST_PCT,
       a.STL_PCT,
       a.BLK_PCT,
       a.TOV_PCT,
       a.USG_PCT,
       a.ORTG,
       a.DRTG
from nba_stats.advanced_box_stats as a
inner join nba_stats.box_score_map as bm on a.game_hash = bm.game_hash
inner join(

     select p.player_id, name, pm.team, lu.day
     from nba_stats.player_info as p
     inner join nba_stats.player_team_map as pm on p.player_id = pm.player_id
     inner join nba_stats.game_date_lookup as lu on pm.season = lu.season

) as p on a.name = p.name and
          a.team = p.team and
          bm.game_date = p.day);
