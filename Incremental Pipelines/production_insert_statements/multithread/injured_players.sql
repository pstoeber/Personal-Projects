insert into nba_stats_prod.injured_players(
select player_id, name, team, source_link, created_at from nba_stats.injuries);
