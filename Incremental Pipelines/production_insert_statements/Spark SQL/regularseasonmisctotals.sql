select player_id,
       r.`SEASON`    AS `season`,
       t.`TEAM_id`      AS `team_id`,
       r.`DBLDBL`    AS `dbldbl`,
       r.`TRIDBL`    AS `tridbl`,
       r.`DQ`        AS `dq`,
       r.`EJECT`     AS `eject`,
       r.`TECH`      AS `tech`,
       r.`to`      AS `flag`,
       r.`AST/TO`    AS `ast_to`,
       r.`STL/TO`    AS `stl_to`,
       r.`RAT`       AS `rat`,
       r.`SCEFF`     AS `sceff`,
       r.`SHEFF`     AS `sheff`,
       r.created_at
from `regularseasonmisctotals` as r
inner join team_info as t on t.team = r.team
where ((r.`TEAM` <> '--') and
        r.`PLAYER_ID` in
        (select `player_id`
        from `player_info`))
