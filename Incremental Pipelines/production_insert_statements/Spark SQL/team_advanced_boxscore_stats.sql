select `b_map`.`game_hash` AS `game_hash`,
        `t`.`TEAM_id`          AS `team_id`,
        `a`.`W/L`           AS `win_lose`,
        `a`.`MIN`           AS `game_length`,
        `a`.`OFFRTG`        AS `off_rating`,
        `a`.`DEFRTG`        AS `def_rating`,
        `a`.`NETRTG`        AS `net_rating`,
        `a`.`AST%` / 100          AS `ast_pct`,
        `a`.`AST/TO`        AS `ast_to_to`,
        `a`.`AST_RATIO`     AS `ast_ratio`,
        `a`.`OREB%` / 100        AS `offensive_reb_pct`,
        `a`.`DREB%` / 100        AS `defensive_reb_pct`,
        `a`.`REB%` / 100         AS `reb_pct`,
        `a`.`TOV%` / 100         AS `to_pct`,
        `a`.`EFG%` / 100         AS `effective_fg_pct`,
        `a`.`TS%`  / 100         AS `ts_pct`,
        `a`.`PACE`          AS `pace`,
        `a`.`PIE`           AS `PIE`,
        `a`.source_link,
        `a`.created_at
  from ((

      select game_hash, home_team, game_date
      from box_score_map

  ) as `b_map`
        join `team_advanced_boxscore_stats` `a`
             on (((`b_map`.`home_team` = `a`.`TEAM`) and
                  (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
        inner join team_info as t on a.team = t.team

  union

  select `b_map`.`game_hash` AS `game_hash`,
        `t`.`TEAM_id`          AS `team_id`,
        `a`.`W/L`           AS `win_lose`,
        `a`.`MIN`           AS `game_length`,
        `a`.`OFFRTG`        AS `off_rating`,
        `a`.`DEFRTG`        AS `def_rating`,
        `a`.`NETRTG`        AS `net_rating`,
        `a`.`AST%` / 100          AS `ast_pct`,
        `a`.`AST/TO`        AS `ast_to_to`,
        `a`.`AST_RATIO`     AS `ast_ratio`,
        `a`.`OREB%` / 100        AS `offensive_reb_pct`,
        `a`.`DREB%` / 100        AS `defensive_reb_pct`,
        `a`.`REB%` / 100         AS `reb_pct`,
        `a`.`TOV%` / 100         AS `to_pct`,
        `a`.`EFG%` / 100         AS `effective_fg_pct`,
        `a`.`TS%`  / 100         AS `ts_pct`,
        `a`.`PACE`          AS `pace`,
        `a`.`PIE`           AS `PIE`,
        `a`.source_link,
        `a`.created_at
  from ((

      select game_hash, away_team, game_date
      from box_score_map

  ) as `b_map`
        join `team_advanced_boxscore_stats` `a`
             on (((`b_map`.`away_team` = `a`.`TEAM`) and
                  (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
        inner join team_info as t on a.team = t.team
