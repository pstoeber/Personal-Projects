select box_m.game_hash,
       t1.team_id as away_team_id,
       t2.team_id as home_team_id,
       box_m.game_date,
       box_m.source_link,
       box_m.created_at,
       f.rowNumber() as rowNumber
from box_score_map as box_m
inner join team_info as t1 on box_m.away_team = t1.team
inner join team_info as t2 on box_m.home_team = t2.team
where box_m.away_team in (select team from team_info) and
     box_m.home_team in (select team from team_info)
