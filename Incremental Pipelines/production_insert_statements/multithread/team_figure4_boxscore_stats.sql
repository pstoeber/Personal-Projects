insert into nba_stats_prod.team_figure4_boxscore_stats select `b_map`.`game_hash`                                   AS `game_hash`,
                                                       `t`.`TEAM_id`                                            AS `team_id`,
                                                       `a`.`W/L`                                             AS `W/L`,
                                                       `a`.`MIN`                                             AS `min`,
                                                       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100     AS `cast(substring(a.``EFG%``, 1, 4) as decimal(10,1))`,
                                                       `a`.`FTA_RATE`                                        AS `fta_rate`,
                                                       `a`.`TOV%` / 100                                           AS `TOV%`,
                                                       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100   AS `My_exp_cast(substring(a.``EFG%``, 1, 4) as decimal(10,1))`,
                                                       cast(substr(`a`.`OPP_EFG%`, 1, 4) as decimal(10, 1)) / 100  AS `cast(substring(a.``opp_efg%``, 1, 4) as decimal(10,1))`,
                                                       `a`.`OPP_FTA_RATE`                                    AS `opp_fta_rate`,
                                                       `a`.`OPP_TOV%`  / 100                                      AS `opp_tov%`,
                                                       cast(substr(`a`.`OPP_OREB%`, 1, 4) as decimal(10, 1)) / 100 AS `cast(substring(a.``OPP_OREB%``, 1, 4) as decimal(10,1))`,
                                                       `a`.source_link,
                                                       `a`.created_at
                                                from (`nba_stats`.`box_score_map_view` `b_map`
                                                       join `nba_stats`.`figure4_team_boxscore_stats` `a`
                                                            on (((`b_map`.`home_team` = `a`.`TEAM`) and
                                                                 (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                       inner join nba_stats.team_info as t on a.team = t.team
                                                union
                                                select `b_map`.`game_hash`                                   AS `game_hash`,
                                                       `t`.`TEAM_id`                                            AS `team_id`,
                                                       `a`.`W/L`                                             AS `W/L`,
                                                       `a`.`MIN`                                             AS `min`,
                                                       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100     AS `cast(substring(a.``EFG%``, 1, 4) as decimal(10,1))`,
                                                       `a`.`FTA_RATE`                                        AS `fta_rate`,
                                                       `a`.`TOV%` / 100                                           AS `TOV%`,
                                                       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100   AS `My_exp_cast(substring(a.``EFG%``, 1, 4) as decimal(10,1))`,
                                                       cast(substr(`a`.`OPP_EFG%`, 1, 4) as decimal(10, 1)) / 100  AS `cast(substring(a.``opp_efg%``, 1, 4) as decimal(10,1))`,
                                                       `a`.`OPP_FTA_RATE`                                    AS `opp_fta_rate`,
                                                       `a`.`OPP_TOV%`  / 100                                      AS `opp_tov%`,
                                                       cast(substr(`a`.`OPP_OREB%`, 1, 4) as decimal(10, 1)) / 100 AS `cast(substring(a.``OPP_OREB%``, 1, 4) as decimal(10,1))`,
                                                       `a`.source_link,
                                                       `a`.created_at
                                                from (`nba_stats`.`box_score_map_view` `b_map`
                                                       join `nba_stats`.`figure4_team_boxscore_stats` `a`
                                                            on (((`b_map`.`away_team` = `a`.`TEAM`) and
                                                                 (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                       inner join nba_stats.team_info as t on a.team = t.team;
