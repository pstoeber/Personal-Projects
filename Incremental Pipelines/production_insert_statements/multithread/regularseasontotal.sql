insert into nba_stats_prod.regularseasontotals (
  select distinct PLAYER_ID AS player_id,
                          SEASON AS season,
                          t.team_id AS team_id,
                          case
                              when `fgm-a` = '' or `fgm-a` is null then '0'
                              else cast(substr(`FGM-A`, 1, locate('-', `FGM-A`) - 1) as unsigned)
                              end AS FG_M,
                          case
                              when `fgm-a` = '' or `fgm-a` is null then '0'
                              else cast(substr(`FGM-A`, (locate('-', `FGM-A`) + 1)) as unsigned)
                              end AS FG_A,
                          `FG%` AS `fg%`,
                          case
                              when `3pm-a` is null or `3pm-a` = '' then '0'
                              else cast(substr(`3PM-A`, 1, locate('-', `3pm-A`) - 1) as unsigned)
                              end AS 3P_M,
                          case
                              when `3pm-a` is null or `3pm-a` = '' then '0'
                              else cast(substr(`3PM-A`, (locate('-', `3PM-A`) + 1)) as unsigned)
                              end AS 3P_A,
                          `3P%` AS `3p%`,
                          case
                              when `ftm-a` is null or `ftm-a` = '' then '0'
                              else cast(substr(`FTM-A`, 1, locate('-', `FTM-A`) - 1) as unsigned)
                              end AS FT_M,
                          case
                              when `ftm-a` is null or `ftm-a` = '' then '0'
                              else cast(substr(`FTM-A`, locate('-', `FTM-A`) + 1) as unsigned)
                              end AS FT_A,
                          `FT%`,
                          `OR`,
                          DR,
                          REB,
                          AST,
                          BLK,
                          STL,
                          PF,
                          `TO`,
                          PTS,
                          created_at
  from nba_stats.regularseasontotals as r
  inner join nba_stats.team_info as t on t.team = r.team
  where ((r.TEAM <> '--') and
        r.PLAYER_ID in
        (select player_info_view.player_id from nba_stats.player_info_view)));
