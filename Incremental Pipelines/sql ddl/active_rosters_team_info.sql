select info.team, info.team_id, stand.conference
from nba_stats.team_info as info
inner join nba_stats.team_standings as stand on info.team = stand.team
where stand.season = (select max(season) from nba_stats.team_standings);
