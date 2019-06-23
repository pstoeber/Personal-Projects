select `b_map`.`game_hash` AS `game_hash`,
       `t`.`TEAM_id`          AS `team_id`,
       `a`.`W/L`           AS `win_lose`,
       `a`.`MIN`           AS `game_length`,
       `a`.`PTS`           AS `pts`,
       `a`.`FGM`           AS `fgm`,
       `a`.`FGA`           AS `fga`,
       `a`.`FG%` / 100           AS `FG_pct`,
       `a`.`3PM`           AS `3pm`,
       `a`.`3PA`           AS `3pa`,
       `a`.`3P%` / 100          AS `3P_pct`,
       `a`.`FTM`           AS `ftm`,
       `a`.`FTA`           AS `fta`,
       `a`.`FT%` / 100           AS `FT_pct`,
       `a`.`OREB`          AS `oreb`,
       `a`.`DREB`          AS `dreb`,
       `a`.`REB`           AS `tot_reb`,
       `a`.`AST`           AS `ast`,
       `a`.`TOV`           AS `tov`,
       `a`.`STL`           AS `stl`,
       `a`.`BLK`           AS `blk`,
       `a`.`PF`            as personal_fouls,
       `a`.`+/-`           AS `plus_minus`,
       `a`.source_link,
       `a`.created_at
from ((

    select game_hash, home_team, game_date
    from box_score_map

) as `b_map`
       join `team_traditional_boxscore_stats` `a`
            on (((`b_map`.`home_team` = `a`.`HOME_TEAM`) and
                 (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
        inner join team_info as t on a.team = t.team
union
select `b_map`.`game_hash` AS `game_hash`,
       `t`.`team_id`          AS `team_id`,
       `a`.`W/L`           AS `win_lose`,
       `a`.`MIN`           AS `game_length`,
       `a`.`PTS`           AS `pts`,
       `a`.`FGM`           AS `fgm`,
       `a`.`FGA`           AS `fga`,
       `a`.`FG%` / 100          AS `FG_pct`,
       `a`.`3PM`           AS `3pm`,
       `a`.`3PA`           AS `3pa`,
       `a`.`3P%` / 100          AS `3P_pct`,
       `a`.`FTM`           AS `ftm`,
       `a`.`FTA`           AS `fta`,
       `a`.`FT%` / 100          AS `FT_pct`,
       `a`.`OREB`          AS `oreb`,
       `a`.`DREB`          AS `dreb`,
       `a`.`REB`           AS `tot_reb`,
       `a`.`AST`           AS `ast`,
       `a`.`TOV`           AS `tov`,
       `a`.`STL`           AS `stl`,
       `a`.`BLK`           AS `blk`,
       `a`.`PF`            as personal_fouls,
       `a`.`+/-`           AS `plus_minus`,
       `a`.source_link,
       `a`.created_at
from ((

    select game_hash, away_team, game_date
    from box_score_map

) as `b_map`
       join `team_traditional_boxscore_stats` `a`
            on (((`b_map`.`away_team` = `a`.`AWAY_TEAM`) and
                 (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
        inner join team_info as t on a.team = t.team
