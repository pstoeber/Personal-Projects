select distinct PLAYER_ID AS player_id,
                        SEASON AS season,
                        t.team_id AS team_id,
                        case
                            when `fgm-a` = '' or `fgm-a` is null then '0'
                            else cast(substr(`FGM-A`, 1, locate('-', `FGM-A`) - 1) as decimal(4,1))
                            end AS FG_M,
                        case
                            when `fgm-a` = '' or `fgm-a` is null then '0'
                            else cast(substr(`FGM-A`, (locate('-', `FGM-A`) + 1)) as decimal(4,1))
                            end AS FG_A,
                        `FG%` AS `fg_pct`,
                        case
                            when `3pm-a` is null or `3pm-a` = '' then '0'
                            else cast(substr(`3PM-A`, 1, locate('-', `3pm-A`) - 1) as decimal(4,1))
                            end AS 3P_M,
                        case
                            when `3pm-a` is null or `3pm-a` = '' then '0'
                            else cast(substr(`3PM-A`, (locate('-', `3PM-A`) + 1)) as decimal(4,1))
                            end AS 3P_A,
                        `3P%` AS `3p_pct`,
                        case
                            when `ftm-a` is null or `ftm-a` = '' then '0'
                            else cast(substr(`FTM-A`, 1, locate('-', `FTM-A`) - 1) as decimal(4,1))
                            end AS FT_M,
                        case
                            when `ftm-a` is null or `ftm-a` = '' then '0'
                            else cast(substr(`FTM-A`, locate('-', `FTM-A`) + 1) as decimal(4,1))
                            end AS FT_A,
                        `FT%` as ft_pct,
                        `OR`,
                        DR,
                        REB,
                        AST,
                        BLK,
                        STL,
                        PF,
                        `TO`,
                        PTS,
                        r.created_at
from regularseasontotals as r
inner join team_info as t on t.team = r.team
where ((r.TEAM <> '--') and
      r.PLAYER_ID in
      (select player_id from player_info))
