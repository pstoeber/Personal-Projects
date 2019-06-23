insert into nba_stats_prod.player_team_map (
  select `p`.`PLAYER_ID` AS `player_id`,
         `r`.`SEASON` AS `season`,
         `t`.`TEAM_id` AS `team`
  from ((`nba_stats`.`player_info` `p`
  join `nba_stats`.`regularseasonaverages` `r` on ((`p`.`PLAYER_ID` = `r`.`PLAYER_ID`)))
  join `nba_stats`.`team_info` `t` on ((`r`.`TEAM` = `t`.`TEAM`)))
  group by `p`.`PLAYER_ID`,`r`.`SEASON`,`r`.`TEAM`);
