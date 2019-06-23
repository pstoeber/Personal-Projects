insert into nba_stats_prod.player_usage_stats(
  select b.game_hash,
         id.player_id,
         t.team_id,
         u.`USG%` / 100,
         u.`%FGM` / 100,
         u.`%FGA` / 100,
         u.`%3PM` / 100,
         u.`%3PA` / 100,
         u.`%FTM` / 100,
         u.`%FTA` / 100,
         u.`%OREB` / 100,
         u.`%DREB` / 100,
         u.`%REB` / 100,
         u.`%AST` / 100,
         u.`%TOV` / 100,
         u.`%STL` / 100,
         u.`%BLK` / 100,
         u.`%BLKA` / 100,
         u.`%PF` / 100,
         u.`%PFD` / 100,
         u.`%PTS` / 100,
         u.source_link,
         u.created_at
  from(

        select box_score_map_view.game_hash,
               box_score_map_view.home_team as team,
               box_score_map_view.game_date
        from nba_stats.box_score_map_view

        union

        select box_score_map_view.game_hash,
               box_score_map_view.away_team as team,
               box_score_map_view.game_date
        from nba_stats.box_score_map_view

      ) as b
  inner join(

        select p.name,
               p.player_id,
               pv.team,
               lu.season,
               lu.day as game_date
        from player_info as p
        inner join nba_stats.player_team_map as pv on p.player_id = pv.player_id
        inner join nba_stats.game_date_lookup as lu on pv.season = lu.season


  ) as id on b.team = id.team and
             b.game_date = id.game_date

  inner join nba_stats.player_usage_stats as u on id.team = u.team and
                                                  id.NAME = u.name and
                                                  id.game_date = u.game_date
  inner join nba_stats.team_info as t on u.team = t.team
);
