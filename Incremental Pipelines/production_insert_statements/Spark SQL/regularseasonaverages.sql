select distinct player_id,
                        season,
                        t.team_id,
                        gp,
                        gs,
                        min,
                       case
                         when `fgm-a` = '' or `fgm-a` is null then '0'
                         else replace(cast(substr(`FGM-A`, 1, locate('-', `FGM-A`) - 1) as decimal(4, 1)), 0.0, 0)
                         end as fg_m,
                      case
                         when `fgm-a` = '' or `fgm-a` is null then '0'
                         else replace(cast(substr(`FGM-A`, locate('-', `FGM-A`) + 1) as decimal(4, 1)), 0.0, 0)
                         end as fg_a,
                       `fg%` as fg_pct,
                       case
                         when `3PM-A` = '' or `3pm-a` is null then '0'
                         else replace(cast(substr(`3pM-A`, 1, locate('-', `3pM-A`) - 1) as decimal(4, 1)), 0.0, 0)
                         end as 3p_m,
                      case
                         when `3pm-a` = '' or `3pm-a` is null then '0'
                         else replace(cast(substr(`3pM-A`, locate('-', `3pM-A`) + 1) as decimal(4, 1)), 0.0, 0)
                         end as 3p_a,
                       `3p%` as 3p_pct,
                       case
                         when `ftm-a` = '' or `ftm-a` is null then '0'
                         else replace(cast(substr(`FtM-A`, 1, locate('-', `FtM-A`) - 1) as decimal(4, 1)), 0.0, 0)
                         end as ft_m,
                      case
                         when `ftm-a` = '' or `ftm-a` is null then '0'
                         else replace(cast(substr(`ftM-A`, locate('-', `ftM-A`) + 1) as decimal(4, 1)), 0.0, 0)
                         end as ft_a,
                       `ft%` as ft_pct,
                       `or`,
                       dr,
                       reb,
                       ast,
                       blk,
                       stl,
                       pf,
                      `to`,
                       pts,
                       r.created_at
        from regularseasonaverages as r
        inner join team_info as t on t.team = r.team
        where r.team != "--"  and
             player_id in (select player_id from player_info)
        order by player_id asc
