insert into nba_stats_prod.player_scoring_stats(
  select b.game_hash,
         id.player_id,
         t.team_id,
         score.`%FGA2PT` / 100,
         score.`%FGA3PT` / 100,
         score.`%PTS2PT` / 100,
         score.`%PTS2PT MR` / 100,
         score.`%PTS3PT` / 100,
         score.`%PTSFBPs` / 100,
         score.`%PTSFT` / 100,
         score.`%PTSOffTO` / 100,
         score.`%PTSPITP` / 100,
         score.`2FGM%AST` / 100,
         score.`2FGM%UAST` / 100,
         score.`3FGM%AST` / 100,
         score.`3FGM%UAST` / 100,
         score.`FGM%AST` / 100,
         score.`FGM%UAST` / 100,
         score.source_link,
         score.created_at
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
  inner join nba_stats.team_info as t on score.team = t.team
);
