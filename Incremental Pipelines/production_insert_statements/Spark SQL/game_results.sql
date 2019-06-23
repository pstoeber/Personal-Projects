select distinct `game_hash`  AS `game_hash`,
                `away_score` AS `away_score`,
                `home_score` AS `home_score`,
                created_at
from `game_results`
where `game_hash` in
       (select `game_hash`
        from `box_score_map`)
