insert into nba_stats_prod.team_scoring_boxscore_stats select `b_map`.`game_hash` AS `game_hash`,
                                                       `t`.`TEAM_id`          AS `team`,
                                                       `a`.`W/L`           AS `W/L`,
                                                       `a`.`MIN`           AS `min`,
                                                       `a`.`%FGA_2PT` / 100     AS `%fga_2pt`,
                                                       `a`.`%FGA_3PT` / 100     AS `%FGA_3PT`,
                                                       `a`.`%PTS_2PT` / 100     AS `%PTS_2PT`,
                                                       `a`.`%PTS_2PT_MR` / 100  AS `%PTS_2PT_MR`,
                                                       `a`.`%PTS_3PT`  / 100    AS `%PTS_3PT`,
                                                       `a`.`%PTS_FBPS` / 100    AS `%PTS_FBPS`,
                                                       `a`.`%PTS_FT` / 100      AS `%PTS_FT`,
                                                       `a`.`%PTS_OFF_TO` / 100  AS `%PTS_OFF_TO`,
                                                       `a`.`%PTS_PITP` / 100    AS `%PTS_PITP`,
                                                       `a`.`2FGM_%AST` / 100    AS `2FGM_%AST`,
                                                       `a`.`2FGM_%UAST` / 100   AS `2FGM_%UAST`,
                                                       `a`.`3FGM_%AST`  / 100   AS `3FGM_%AST`,
                                                       `a`.`3FGM_%UAST` / 100   AS `3FGM_%UAST`,
                                                       `a`.`FGM_%AST` / 100     AS `FGM_%AST`,
                                                       `a`.`FGM_%UAST` / 100    AS `FGM_%UAST`,
                                                       `a`.source_link,
                                                       `a`.created_at
                                                from (`nba_stats`.`box_score_map_view` `b_map`
                                                       join `nba_stats`.`team_scoring_boxscore_stats` `a`
                                                            on (((`b_map`.`home_team` = `a`.`TEAM`) and
                                                                 (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                        inner join nba_stats.team_info as t on a.team = t.team
                                                union
                                                select `b_map`.`game_hash` AS `game_hash`,
                                                       `t`.`TEAM_id`          AS `team`,
                                                       `a`.`W/L`           AS `W/L`,
                                                       `a`.`MIN`           AS `min`,
                                                       `a`.`%FGA_2PT` / 100     AS `%fga_2pt`,
                                                       `a`.`%FGA_3PT` / 100     AS `%FGA_3PT`,
                                                       `a`.`%PTS_2PT` / 100     AS `%PTS_2PT`,
                                                       `a`.`%PTS_2PT_MR` / 100  AS `%PTS_2PT_MR`,
                                                       `a`.`%PTS_3PT`  / 100    AS `%PTS_3PT`,
                                                       `a`.`%PTS_FBPS` / 100    AS `%PTS_FBPS`,
                                                       `a`.`%PTS_FT` / 100      AS `%PTS_FT`,
                                                       `a`.`%PTS_OFF_TO` / 100  AS `%PTS_OFF_TO`,
                                                       `a`.`%PTS_PITP` / 100    AS `%PTS_PITP`,
                                                       `a`.`2FGM_%AST` / 100    AS `2FGM_%AST`,
                                                       `a`.`2FGM_%UAST` / 100   AS `2FGM_%UAST`,
                                                       `a`.`3FGM_%AST`  / 100   AS `3FGM_%AST`,
                                                       `a`.`3FGM_%UAST` / 100   AS `3FGM_%UAST`,
                                                       `a`.`FGM_%AST` / 100     AS `FGM_%AST`,
                                                       `a`.`FGM_%UAST` / 100    AS `FGM_%UAST`,
                                                       `a`.source_link,
                                                       `a`.created_at
                                                from (`nba_stats`.`box_score_map_view` `b_map`
                                                       join `nba_stats`.`team_scoring_boxscore_stats` `a`
                                                            on (((`b_map`.`away_team` = `a`.`TEAM`) and
                                                                 (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                        inner join nba_stats.team_info as t on a.team = t.team;
