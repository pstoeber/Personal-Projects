insert into nba_stats_prod.regularseasonmisctotals (select distinct r.`PLAYER_ID` AS `player_id`,
                                                      r.`SEASON`    AS `season`,
                                                       t.`TEAM_id`      AS `team`,
                                                       r.`DBLDBL`    AS `dbldbl`,
                                                       r.`TRIDBL`    AS `tridbl`,
                                                       r.`DQ`        AS `dq`,
                                                       r.`EJECT`     AS `eject`,
                                                       r.`TECH`      AS `tech`,
                                                       r.`FLAG`      AS `flag`,
                                                       r.`AST/TO`    AS `ast/to`,
                                                       r.`STL/TO`    AS `stl/to`,
                                                       r.`RAT`       AS `rat`,
                                                       r.`SCEFF`     AS `sceff`,
                                                       r.`SHEFF`     AS `sheff`,
                                                       created_at
                                       from `nba_stats`.`regularseasonmisctotals` as r
                                       inner join nba_stats.team_info as t on t.team = r.team
                                       where ((r.`TEAM` <> '--') and
                                              r.`PLAYER_ID` in
                                              (select `player_info_view`.`player_id`
                                               from `nba_stats`.`player_info_view`)));
