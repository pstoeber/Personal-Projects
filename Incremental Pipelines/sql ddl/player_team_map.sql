insert into nba_stats.player_team_map(
  select p.player_id,
         r.SEASON,
         t.TEAM,
         t.team_id
from nba_stats.player_info as p
inner join nba_stats.regularseasonaverages as r on p.player_id = r.PLAYER_ID
inner join nba_stats.team_info as t on r.TEAM = t.TEAM
group by p.player_id,
         r.SEASON,
         r.TEAM);
