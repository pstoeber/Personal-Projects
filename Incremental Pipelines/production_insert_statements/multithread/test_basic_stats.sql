insert into nba_stats_prod.basic_box_stats(
  select b.game_hash,
       p.player_id,
       b.team,
       b.MP,
       b.FG,
       b.FGA,
       b.fg_pct,
       b.3P,
       b.3PA,
       b.3p_pct,
       b.FT,
       b.ft_pct,
       b.ORB,
       b.DRB,
       b.TRB,
       b.AST,
       b.STL,
       b.BLK,
       b.TOV,
       b.PF,
       b.PTS,
       b.plus_minus
from nba_stats.basic_box_stats as b
inner join nba_stats.box_score_map as bm on b.game_hash = bm.game_hash
inner join(

     select p.player_id, name, pm.team, lu.day
     from nba_stats.player_info as p
     inner join nba_stats.player_team_map as pm on p.player_id = pm.player_id
     inner join nba_stats.game_date_lookup as lu on pm.season = lu.season

) as p on b.name = p.name and
          b.team = p.team and
          bm.game_date = p.day);
