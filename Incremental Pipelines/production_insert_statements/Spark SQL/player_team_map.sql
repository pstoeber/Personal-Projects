  select `p`.`PLAYER_ID` AS `player_id`,
         `r`.`SEASON` AS `season`,
         `t`.`TEAM_id` AS `team_id`
  from ((`player_info` `p`
  join `regularseasonaverages` `r` on ((`p`.`PLAYER_ID` = `r`.`PLAYER_ID`)))
  join `team_info` `t` on ((`r`.`TEAM` = `t`.`TEAM`)))
  group by `p`.`PLAYER_ID`,`r`.`SEASON`,`t`.`TEAM_id`
