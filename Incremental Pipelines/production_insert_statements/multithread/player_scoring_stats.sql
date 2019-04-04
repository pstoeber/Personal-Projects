insert into nba_stats_prod.player_scoring_stats(
  select b.game_hash,
         id.player_id,
         score.team,
         score.`%FGA2PT`,
         score.`%FGA3PT`,
         score.`%PTS2PT`,
         score.`%PTS2PT MR`,
         score.`%PTS3PT`,
         score.`%PTSFBPs`,
         score.`%PTSFT`,
         score.`%PTSOffTO`,
         score.`%PTSPITP`,
         score.`2FGM%AST`,
         score.`2FGM%UAST`,
         score.`3FGM%AST`,
         score.`3FGM%UAST`,
         score.`FGM%AST`,
         score.`FGM%UAST`
  from(

        select box_score_map_view.game_hash,
               box_score_map_view.home_team as team,
               box_score_map_view.game_date
        from nba_stats.box_score_map_view

        union

        select box_score_map_view.game_hash,
               box_score_map_view.away_team as team,
               box_score_map_view.game_date
        from nba_stats.box_score_map_view

      ) as b
  inner join(

        select p.name,
               p.player_id,
               pv.team,
               lu.season,
               lu.day as game_date
        from player_info as p
        inner join nba_stats.player_team_map as pv on p.player_id = pv.player_id
        inner join nba_stats.game_date_lookup as lu on pv.season = lu.season


  ) as id on b.team = id.team and
             b.game_date = id.game_date

  inner join nba_stats.player_scoring_stats as score on id.team = score.team and
                                                        id.NAME = score.name and
                                                        id.game_date = score.game_date
);
