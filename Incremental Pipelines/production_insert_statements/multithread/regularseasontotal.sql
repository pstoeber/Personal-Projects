insert into nba_stats_prod.regularseasontotals (
  select distinct nba_stats.regularseasontotals.PLAYER_ID                                               AS player_id,
                nba_stats.regularseasontotals.SEASON                                                  AS season,
               nba_stats.regularseasontotals.TEAM                                                    AS team,
                cast(substr(nba_stats.regularseasontotals.`FGM-A`, 1,
                           locate('-', nba_stats.regularseasontotals.`FGM-A`)) as unsigned)       AS FG_A,
               cast(substr(nba_stats.regularseasontotals.`FGM-A`,
                           (locate('-', nba_stats.regularseasontotals.`FGM-A`) + 1)) as unsigned) AS FG_M,
               nba_stats.regularseasontotals.`FG%`                                                     AS `fg%`,
               cast(substr(nba_stats.regularseasontotals.`3PM-A`, 1,
                           locate('-', nba_stats.regularseasontotals.`FGM-A`)) as unsigned)       AS 3P_A,
               cast(substr(nba_stats.regularseasontotals.`3PM-A`,
                           (locate('-', nba_stats.regularseasontotals.`FGM-A`) + 1)) as unsigned) AS 3P_M,
               nba_stats.regularseasontotals.`3P%`                                                     AS `3p%`,
               cast(substr(nba_stats.regularseasontotals.`FTM-A`, 1,
                           locate('-', nba_stats.regularseasontotals.`FGM-A`)) as unsigned)       AS FT_A,
               cast(substr(nba_stats.regularseasontotals.`FTM-A`, 1,
                           locate('-', nba_stats.regularseasontotals.`FGM-A`)) as unsigned)       AS FT_M,
               nba_stats.regularseasontotals.`FT%`                                                     AS `ft%`,
               nba_stats.regularseasontotals.OR                                                      AS `or`,
               nba_stats.regularseasontotals.DR                                                      AS dr,
               nba_stats.regularseasontotals.REB                                                     AS reb,
               nba_stats.regularseasontotals.AST                                                     AS ast,
               nba_stats.regularseasontotals.BLK                                                     AS blk,
               nba_stats.regularseasontotals.STL                                                     AS stl,
               nba_stats.regularseasontotals.PF                                                      AS pf,
               nba_stats.regularseasontotals.TO                                                      AS `to`,
               nba_stats.regularseasontotals.PTS                                                     AS pts
from nba_stats.regularseasontotals
where ((nba_stats.regularseasontotals.TEAM <> '--') and
      nba_stats.regularseasontotals.PLAYER_ID in
      (select player_info_view.player_id from nba_stats.player_info_view)));
