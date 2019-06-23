select b.game_hash,
       id.player_id,
       t.team_id,
       score.`%FGA2PT` / 100 as pct_2pt_fga,
       score.`%FGA3PT` / 100 as pct_3pt_fga,
       score.`%PTS2PT` / 100 as pct_pt_2pt,
       score.`%PTS2PT MR` / 100 as pct_pts_2pt_mr,
       score.`%PTS3PT` / 100 as pct_pts_3pt,
       score.`%PTSFBPs` / 100 as pct_pts_fbps,
       score.`%PTSFT` / 100 as pct_pts_ft,
       score.`%PTSOffTO` / 100 as pct_pts_off_to,
       score.`%PTSPITP` / 100 as pct_pts_pitp,
       score.`2FGM%AST` / 100 as 2pt_fgm_pct_ast,
       score.`2FGM%UAST` / 100 as 2pt_fgm_pct_uast,
       score.`3FGM%AST` / 100 as 3pt_fgm_pct_ast,
       score.`3FGM%UAST` / 100 as 3pt_fgm_pct_uast,
       score.`FGM%AST` / 100 as fgm_pct_ast,
       score.`FGM%UAST` / 100 as fgm_pct_uast,
       score.source_link,
       score.created_at
from(

      select game_hash,
             home_team as team,
             game_date
      from box_score_map

      union

      select game_hash,
             away_team as team,
             game_date
      from box_score_map

    ) as b
inner join(

      select p.name,
             p.player_id,
             pv.team,
             lu.season,
             lu.day as game_date
      from player_info as p
      inner join player_team_map as pv on p.player_id = pv.player_id
      inner join game_date_lookup as lu on pv.season = lu.season


) as id on b.team = id.team and
           b.game_date = id.game_date

inner join player_scoring_stats as score on id.team = score.team and
                                                      id.NAME = score.name and
                                                      id.game_date = score.game_date
inner join team_info as t on score.team = t.team
