select map.player_id, map.team, p.name
from player_team_map as map
inner join player_info as p on map.player_id = p.player_id
where season = (select max(season) from player_team_map) and
      p.name in ("{}")
