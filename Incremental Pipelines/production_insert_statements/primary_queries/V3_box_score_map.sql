insert into nba_stats_prod.box_score_map(
  select box_m.game_hash,
       t1.team_id as away_team,
       t2.team_id as home_team,
       box_m.game_date,
       box_m.source_link,
       box_m.created_at
 from nba_stats.box_score_map as box_m
 inner join nba_stats.team_info as t1 on box_m.away_team = t1.team
 inner join nba_stats.team_info as t2 on box_m.home_team = t2.team
 where box_m.away_team in (select team from nba_stats.team_info) and
       box_m.home_team in (select team from nba_stats.team_info));
