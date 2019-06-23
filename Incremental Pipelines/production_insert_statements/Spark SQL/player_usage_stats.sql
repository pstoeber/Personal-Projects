select b.game_hash,
       id.player_id,
       t.team_id,
       u.`USG%` / 100 as usage_pct,
       u.`%FGM` / 100 as pct_fgm,
       u.`%FGA` / 100 as pct_fga,
       u.`%3PM` / 100 as pct_3pm,
       u.`%3PA` / 100 as pct_3pa,
       u.`%FTM` / 100 as pct_ftm,
       u.`%FTA` / 100 as pct_fta,
       u.`%OREB` / 100 as pct_oreb,
       u.`%DREB` / 100 as pct_dreb,
       u.`%REB` / 100 as pct_reb,
       u.`%AST` / 100 as pct_ast,
       u.`%TOV` / 100 as pct_tov,
       u.`%STL` / 100 as pct_stl,
       u.`%BLK` / 100 as pct_blk,
       u.`%BLKA` / 100 as pct_blka,
       u.`%PF` / 100 as pct_pf,
       u.`%PFD` / 100 pct_pfd,
       u.`%PTS` / 100 as pct_pts,
       u.source_link,
       u.created_at
from(

      select game_hash,
             home_team as team,
             game_date
      from box_score_map

      union

      select game_hash,
             away_team as team,
             game_date
      from box_score_map

    ) as b
inner join(

      select p.name,
             p.player_id,
             pv.team,
             lu.season,
             lu.day as game_date
      from player_info as p
      inner join player_team_map as pv on p.player_id = pv.player_id
      inner join game_date_lookup as lu on pv.season = lu.season


) as id on b.team = id.team and
           b.game_date = id.game_date

inner join player_usage_stats as u on id.team = u.team and
                                      id.NAME = u.name and
                                      id.game_date = u.game_date
inner join team_info as t on u.team = t.team
