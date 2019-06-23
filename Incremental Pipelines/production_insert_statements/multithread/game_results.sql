insert into nba_stats_prod.game_results select distinct `nba_stats`.`game_results`.`game_hash`  AS `game_hash`,
                                                  `nba_stats`.`game_results`.`away_score` AS `away_score`,
                                                  `nba_stats`.`game_results`.`home_score` AS `home_score`,
                                                  created_at
                                  from `nba_stats`.`game_results`
                                  where ((not (`nba_stats`.`game_results`.`game_hash` in
                                               (select `nba_stats`.`do_not_include_hashes`.`game_hash`
                                                from `nba_stats`.`do_not_include_hashes`))) and
                                         `nba_stats`.`game_results`.`game_hash` in
                                         (select `nba_stats_prod`.`box_score_map`.`game_hash`
                                          from `nba_stats_prod`.`box_score_map`));
