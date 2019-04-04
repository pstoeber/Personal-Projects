insert into nba_stats_prod.player_usage_stats(
  select b.game_hash,
         id.player_id,
         u.team,
         u.`USG%`,
         u.`%FGM`,
         u.`%FGA`,
         u.`%3PM`,
         u.`%3PA`,
         u.`%FTM`,
         u.`%FTA`,
         u.`%OREB`,
         u.`%DREB`,
         u.`%REB`,
         u.`%AST`,
         u.`%TOV`,
         u.`%STL`,
         u.`%BLK`,
         u.`%BLKA`,
         u.`%PF`,
         u.`%PFD`,
         u.`%PTS`
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
);
