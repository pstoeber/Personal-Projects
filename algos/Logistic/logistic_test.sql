select bm.target as team,
       adv.game_length,
       adv.ast_to_to,
       adv.ast_ratio,
       adv.defensive_reb_pct,
       adv.reb_pct,
       adv.to_pct,
       adv.ts_pct,
       adv.pace,
       fig.fta_rate,
       fig.tov_pct,
       fig.opp_tov_pct,
       fig.opp_off_reb_pct,
       misc.points_off_to,
       misc.pts_in_paint,
       misc.opp_pts_off_to,
       misc.opp_second_chance_pts,
       score.pct_2pt_fg,
       score.pct_3pt_fg,
       score.pct_pts_2pt,
       score.pct_pts_2pt_mr,
       score.pct_pts_fbps,
       score.pct_pts_off_to,
       score.fgm_pct_uast,
       trad.fga,
       trad.3pa,
       trad.fta,
       trad.oreb,
       trad.dreb,
       trad.tot_reb,
       trad.ast,
       trad.stl,
       trad.blk,
       trad.personal_fouls,
       3p.ft_pct,
       pts.diff,
       reb.off_reb,
       reb.def_reb,
       reb.tot_reb,
       t.own_to,
       t.opp_to,
       stand.home_loses,
       stand.home_wins,
       stand.away_loses,
       stand.away_wins,
       opp_pts.opp_pts as opp_pts_allowed,
       opp_pts.own_pts as opp_pts_scored
from box_scores_map_view as b
inner join(

          select b.game_hash,
                 map.target,
                 lu.season,
                 target_id.team_id as target_id,
                 opp_id.team_id as opp_id
          from(

            select game_hash, game_date
            from box_scores_map_view
            where team like '{}%'
            order by game_date desc limit 10

              ) as b

          inner join (

              select game_hash, home_team as target, away_team as opp
              from box_score_map
              where home_team like '{}%'

              union

              select game_hash, away_team as target, home_team as opp
              from box_score_map
              where away_team like '{}%'

              ) as map on b.game_hash = map.game_hash

          inner join team_info as target_id on map.target = target_id.team
          inner join team_info as opp_id on map.opp= opp_id.team
          inner join game_date_lookup as lu on b.game_date = lu.day

         ) as bm on ( (b.game_hash = bm.game_hash) and (b.team = bm.target) )

inner join team_advanced_boxscore_stats as adv on ( (bm.target = adv.team) and (bm.game_hash = adv.game_hash) )
inner join team_figure4_boxscore_stats as fig on ( (adv.team = fig.team) and (adv.game_hash = fig.game_hash) )
inner join team_misc_boxscore_stats as misc on ( (fig.team = misc.team) and (fig.game_hash = misc.game_hash) )
inner join team_scoring_boxscore_stats as score on ( (misc.team = score.team) and (misc.game_hash = score.game_hash) )
inner join team_traditional_boxscore_stats as trad on ( (score.team = trad.team) and (score.game_hash = trad.game_hash) )
inner join 3pt_pct as 3p on ( (bm.target_id = 3p.team_id) and (bm.season -1 = 3p.season) )
inner join points as pts on ( (bm.target_id = pts.team_id) and (bm.season -1 = pts.season) )
inner join rebound_pct as reb on ( (bm.target_id = reb.team_id) and (bm.season -1 = reb.season) )
inner join turnovers as t on ( (bm.target_id = t.team_id) and (bm.season -1 = t.season) )
inner join points as opp_pts on ( (bm.opp_id = opp_pts.team_id) and (bm.season -1 = opp_pts.season) )
inner join team_standings as stand on ( (bm.target = stand.team) and (bm.season = stand.season) )
