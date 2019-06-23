insert into nba_stats_prod.team_advanced_boxscore_stats select `b_map`.`game_hash` AS `game_hash`,
                                                        `t`.`TEAM_id`          AS `team`,
                                                        `a`.`W/L`           AS `W/L`,
                                                        `a`.`MIN`           AS `min`,
                                                        `a`.`OFFRTG`        AS `OFFRTG`,
                                                        `a`.`DEFRTG`        AS `DEFRTG`,
                                                        `a`.`NETRTG`        AS `NETRTG`,
                                                        `a`.`AST%` / 100          AS `AST%`,
                                                        `a`.`AST/TO`        AS `AST/TO`,
                                                        `a`.`AST_RATIO`     AS `ast_ratio`,
                                                        `a`.`OREB%` / 100        AS `OREB%`,
                                                        `a`.`DREB%` / 100        AS `DREB%`,
                                                        `a`.`REB%` / 100         AS `REB%`,
                                                        `a`.`TOV%` / 100         AS `TOV%`,
                                                        `a`.`EFG%` / 100         AS `EFG%`,
                                                        `a`.`TS%`  / 100         AS `TS%`,
                                                        `a`.`PACE`          AS `pace`,
                                                        `a`.`PIE`           AS `PIE`,
                                                        `a`.source_link,
                                                        `a`.created_at
                                                 from (`nba_stats`.`box_score_map_view` `b_map`
                                                        join `nba_stats`.`advanced_team_boxscore_stats` `a`
                                                             on (((`b_map`.`home_team` = `a`.`TEAM`) and
                                                                  (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                        inner join nba_stats.team_info as t on a.team = t.team

                                                 union

                                                 select `b_map`.`game_hash` AS `game_hash`,
                                                        `t`.`TEAM_id`          AS `team`,
                                                        `a`.`W/L`           AS `W/L`,
                                                        `a`.`MIN`           AS `min`,
                                                        `a`.`OFFRTG`        AS `OFFRTG`,
                                                        `a`.`DEFRTG`        AS `DEFRTG`,
                                                        `a`.`NETRTG`        AS `NETRTG`,
                                                        `a`.`AST%` / 100          AS `AST%`,
                                                        `a`.`AST/TO`        AS `AST/TO`,
                                                        `a`.`AST_RATIO`     AS `ast_ratio`,
                                                        `a`.`OREB%` / 100        AS `OREB%`,
                                                        `a`.`DREB%` / 100        AS `DREB%`,
                                                        `a`.`REB%` / 100         AS `REB%`,
                                                        `a`.`TOV%` / 100         AS `TOV%`,
                                                        `a`.`EFG%` / 100         AS `EFG%`,
                                                        `a`.`TS%`  / 100         AS `TS%`,
                                                        `a`.`PACE`          AS `pace`,
                                                        `a`.`PIE`           AS `PIE`,
                                                        `a`.source_link,
                                                        `a`.created_at
                                                 from (`nba_stats`.`box_score_map_view` `b_map`
                                                        join `nba_stats`.`advanced_team_boxscore_stats` `a`
                                                             on (((`b_map`.`away_team` = `a`.`TEAM`) and
                                                                  (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                        inner join nba_stats.team_info as t on a.team = t.team
