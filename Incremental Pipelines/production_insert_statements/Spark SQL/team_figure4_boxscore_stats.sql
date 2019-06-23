select `b_map`.`game_hash`                                   AS `game_hash`,
       `t`.`TEAM_id`                                            AS `team_id`,
       `a`.`W/L`                                             AS `win_lose`,
       `a`.`MIN`                                             AS `game_length`,
       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100     AS `effective_fg_pct`,
       `a`.`FTA_RATE`                                        AS `fta_rate`,
       `a`.`TOV%` / 100                                           AS `tov_pct`,
       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100   AS `oreb_pct`,
       cast(substr(`a`.`OPP_EFG%`, 1, 4) as decimal(10, 1)) / 100  AS `opp_effective_fg_pct`,
       `a`.`OPP_FTA_RATE`                                    AS `opp_fta_rate`,
       `a`.`OPP_TOV%`  / 100                                      AS `opp_tov_pct`,
       cast(substr(`a`.`OPP_OREB%`, 1, 4) as decimal(10, 1)) / 100 AS `opp_off_reb_pct`,
       `a`.source_link,
       `a`.created_at
from ((

    select game_hash, home_team, game_date
    from box_score_map

) as `b_map`
       join `team_figure4_boxscore_stats` `a`
            on (((`b_map`.`home_team` = `a`.`TEAM`) and
                 (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
       inner join team_info as t on a.team = t.team
union
select `b_map`.`game_hash`                                   AS `game_hash`,
       `t`.`TEAM_id`                                            AS `team_id`,
       `a`.`W/L`                                             AS `win_lose`,
       `a`.`MIN`                                             AS `game_length`,
       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100     AS `effective_fg_pct`,
       `a`.`FTA_RATE`                                        AS `fta_rate`,
       `a`.`TOV%` / 100                                           AS `tov_pct`,
       cast(substr(`a`.`EFG%`, 1, 4) as decimal(10, 1)) / 100   AS `oreb_pct`,
       cast(substr(`a`.`OPP_EFG%`, 1, 4) as decimal(10, 1)) / 100  AS `opp_effective_fg_pct`,
       `a`.`OPP_FTA_RATE`                                    AS `opp_fta_rate`,
       `a`.`OPP_TOV%`  / 100                                      AS `opp_tov_pct`,
       cast(substr(`a`.`OPP_OREB%`, 1, 4) as decimal(10, 1)) / 100 AS `opp_off_reb_pct`,
       `a`.source_link,
       `a`.created_at
from ((

    select game_hash, away_team, game_date
    from box_score_map

) as `b_map`
       join `team_figure4_boxscore_stats` `a`
            on (((`b_map`.`away_team` = `a`.`TEAM`) and
                 (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
       inner join team_info as t on a.team = t.team
