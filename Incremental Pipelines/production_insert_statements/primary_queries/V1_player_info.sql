 insert into nba_stats_prod.player_info(
   select player_id,
         name,
         team,
         position,
         source_link,
         created_at
   from nba_stats.player_info
  );
