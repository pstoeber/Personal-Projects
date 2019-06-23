select `b_map`.`game_hash`  AS `game_hash`,
      `t`.`TEAM_id`           AS `team_id`,
      `a`.`W/L`            AS `win_lose`,
      `a`.`MIN`            AS `game_length`,
      `a`.`PTS_OFF_TO`     AS `points_off_to`,
      `a`.`2ND_PTS`        AS `second_chance_pts`,
      `a`.`FBPS`           AS `fbps`,
      `a`.`PITP`           AS `pts_in_paint`,
      `a`.`OPP_PTS_OFF_TO` AS `opp_pts_off_to`,
      `a`.`OPP_2ND_PTS`    AS `opp_second_chance_pts`,
      `a`.`OPP_FBPS`       AS `opp_fbps`,
      `a`.`OPP_PITP`       AS `opp_pts_in_paint`,
      `a`.source_link,
      `a`.created_at
from ((

    select game_hash, home_team, game_date
    from box_score_map

) as `b_map`
      join `team_misc_boxscore_stats` `a`
           on (((`b_map`.`home_team` = `a`.`TEAM`) and
                (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
      inner join team_info as t on a.team = t.team
union
select `b_map`.`game_hash`  AS `game_hash`,
      `t`.`TEAM_id`           AS `team_id`,
      `a`.`W/L`            AS `win_lose`,
      `a`.`MIN`            AS `game_length`,
      `a`.`PTS_OFF_TO`     AS `points_off_to`,
      `a`.`2ND_PTS`        AS `second_chance_pts`,
      `a`.`FBPS`           AS `fbps`,
      `a`.`PITP`           AS `pts_in_paint`,
      `a`.`OPP_PTS_OFF_TO` AS `opp_pts_off_to`,
      `a`.`OPP_2ND_PTS`    AS `opp_second_chance_pts`,
      `a`.`OPP_FBPS`       AS `opp_fbps`,
      `a`.`OPP_PITP`       AS `opp_pts_in_paint`,
      `a`.source_link,
      `a`.created_at
from ((

    select game_hash, away_team, game_date
    from box_score_map

) as `b_map`
      join `team_misc_boxscore_stats` `a`
           on (((`b_map`.`away_team` = `a`.`TEAM`) and
                (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
      inner join team_info as t on a.team = t.team
