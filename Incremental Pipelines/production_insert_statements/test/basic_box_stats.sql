select b.game_hash,
       p.player_id,
       t.team_id,
       b.MP as seconds_played,
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
       b.plus_minus,
       b.source_link,
       b.created_at,
       f.rowNumber() as rowNumber
from basic_box_stats as b
inner join(

    select game_hash, game_date
    from box_score_map

  ) as bm on b.game_hash = bm.game_hash
inner join team_info as t on b.team = t.team
inner join(

     select p.player_id, name, pm.team, lu.day
     from player_info as p
     inner join player_team_map as pm on p.player_id = pm.player_id
     inner join game_date_lookup as lu on pm.season = lu.season

) as p on b.name = p.name and
          b.team = p.team and
          bm.game_date = p.day
