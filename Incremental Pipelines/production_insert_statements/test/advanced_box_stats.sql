select a.game_hash,
       p.player_id,
       t.team_id,
       a.MP as seconds_played,
       a.TS_PCT as true_shooting_pct,
       a.EFG_PCT as effective_fg_pct,
       a.3PAR 3P_attempt_rate,
       a.FTR FT_attempt_rate,
       a.ORB_PCT offensive_reb_rate,
       a.DRB_PCT defensive_reb_rate,
       a.TRB_PCT total_reb_pct,
       a.AST_PCT as assist_pct,
       a.STL_PCT as steal_pct,
       a.BLK_PCT as block_pct,
       a.TOV_PCT as turnover_pct,
       a.USG_PCT as usage_pct,
       a.ORTG as offensive_rating,
       a.DRTG as defensive_rating,
       a.source_link,
       a.created_at,
       f.rowNumber() as rowNumber
from advanced_box_stats as a
inner join (

    select game_hash, game_date
    from box_score_map

    ) as bm on a.game_hash = bm.game_hash
inner join team_info as t on a.team = t.team
inner join(

     select p.player_id, name, pm.team, lu.day
     from player_info as p
     inner join player_team_map as pm on p.player_id = pm.player_id
     inner join game_date_lookup as lu on pm.season = lu.season

) as p on a.name = p.name and
          a.team = p.team and
          bm.game_date = p.day
