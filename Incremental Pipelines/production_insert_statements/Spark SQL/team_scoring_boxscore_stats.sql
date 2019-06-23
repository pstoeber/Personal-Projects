select `b_map`.`game_hash` AS `game_hash`,
       `t`.`TEAM_id`          AS `team_id`,
       `a`.`W/L`           AS `win_lose`,
       `a`.`MIN`           AS `game_length`,
       `a`.`%FGA_2PT` / 100     AS `pct_2pt_fg`,
       `a`.`%FGA_3PT` / 100     AS `pct_3pt_fg`,
       `a`.`%PTS_2PT` / 100     AS `pct_pts_2pt`,
       `a`.`%PTS_2PT_MR` / 100  AS `pct_pts_2pt_mr`,
       `a`.`%PTS_3PT`  / 100    AS `pct_pts_3pt`,
       `a`.`%PTS_FBPS` / 100    AS `pct_pts_fbps`,
       `a`.`%PTS_FT` / 100      AS `pct_pts_ft`,
       `a`.`%PTS_OFF_TO` / 100  AS `pct_pts_off_to`,
       `a`.`%PTS_PITP` / 100    AS `pct_pts_pitp`,
       `a`.`2FGM_%AST` / 100    AS `2pt_fgm_ast_pct`,
       `a`.`2FGM_%UAST` / 100   AS `2pt_fgm_uast_pct`,
       `a`.`3FGM_%AST`  / 100   AS `3pt_fgm_ast_pct`,
       `a`.`3FGM_%UAST` / 100   AS `3pt_fgm_uast_pct`,
       `a`.`FGM_%AST` / 100     AS `fgm_pct_ast`,
       `a`.`FGM_%UAST` / 100    AS `fgm_pct_uast`,
       `a`.source_link,
       `a`.created_at
from ((

    select game_hash, home_team, game_date
    from box_score_map

) as `b_map`
       join `team_scoring_boxscore_stats` `a`
            on (((`b_map`.`home_team` = `a`.`TEAM`) and
                 (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
        inner join team_info as t on a.team = t.team
union
select `b_map`.`game_hash` AS `game_hash`,
       `t`.`TEAM_id`          AS `team_id`,
       `a`.`W/L`           AS `win_lose`,
       `a`.`MIN`           AS `game_length`,
       `a`.`%FGA_2PT` / 100     AS `pct_2pt_fg`,
       `a`.`%FGA_3PT` / 100     AS `pct_3pt_fg`,
       `a`.`%PTS_2PT` / 100     AS `pct_pts_2pt`,
       `a`.`%PTS_2PT_MR` / 100  AS `pct_pts_2pt_mr`,
       `a`.`%PTS_3PT`  / 100    AS `pct_pts_3pt`,
       `a`.`%PTS_FBPS` / 100    AS `pct_pts_fbps`,
       `a`.`%PTS_FT` / 100      AS `pct_pts_ft`,
       `a`.`%PTS_OFF_TO` / 100  AS `pct_pts_off_to`,
       `a`.`%PTS_PITP` / 100    AS `pct_pts_pitp`,
       `a`.`2FGM_%AST` / 100    AS `2pt_fgm_ast_pct`,
       `a`.`2FGM_%UAST` / 100   AS `2pt_fgm_uast_pct`,
       `a`.`3FGM_%AST`  / 100   AS `3pt_fgm_ast_pct`,
       `a`.`3FGM_%UAST` / 100   AS `3pt_fgm_uast_pct`,
       `a`.`FGM_%AST` / 100     AS `fgm_pct_ast`,
       `a`.`FGM_%UAST` / 100    AS `fgm_pct_uast`,
       `a`.source_link,
       `a`.created_at
from ((

    select game_hash, away_team, game_date
    from box_score_map

) as `b_map`
       join `team_scoring_boxscore_stats` `a`
            on (((`b_map`.`away_team` = `a`.`TEAM`) and
                 (`b_map`.`game_date` = CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(a.game_date, 'yyyy-MM-dd')) AS date)))))
        inner join team_info as t on a.team = t.team
