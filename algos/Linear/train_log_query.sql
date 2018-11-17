select adv.win_lose,
       bm.home_away,
       adv.game_length,
       adv.ast_pct,
       adv.ast_to_to,
       adv.ast_ratio,
       adv.offensive_reb_pct,
       adv.defensive_reb_pct,
       adv.reb_pct,
       adv.to_pct,
       adv.effective_fg_pct,
       adv.ts_pct,
       adv.pace,
       fig.fta_rate,
       fig.tov_pct,
       fig.oreb_pct,
       fig.opp_effective_fg_pct,
       fig.opp_fta_rate,
       fig.opp_tov_pct,
       fig.opp_off_reb_pct,
       misc.points_off_to,
       misc.second_chance_pts,
       misc.fbps,
       misc.pts_in_paint,
       misc.opp_pts_off_to,
       misc.opp_second_chance_pts,
       misc.opp_fbps,
       misc.opp_pts_in_paint,
       score.pct_2pt_fg,
       score.pct_3pt_fg,
       score.pct_pts_2pt,
       score.pct_pts_2pt_mr,
       score.pct_pts_3pt,
       score.pct_pts_fbps,
       score.pct_pts_ft,
       score.pct_pts_off_to,
       score.pct_pts_pitp,
       score.2pt_fgm_ast_pct,
       score.3pt_fgm_ast_pct,
       score.3pt_fgm_uast_pct,
       score.fgm_pct_ast,
       score.fgm_pct_uast,
       trad.fga,
       trad.fg_pct,
       trad.3pa,
       trad.3p_pct,
       trad.fta,
       trad.ft_pct,
       trad.oreb,
       trad.dreb,
       trad.tot_reb,
       trad.ast,
       trad.stl,
       trad.blk,
       trad.personal_fouls
from box_scores_map_view as b
inner join(

    select b.game_hash, b.team, b.home_away, b.game_date
    from box_scores_map_view as b
    inner join game_date_lookup as lu on b.game_date = lu.day
    where b.game_date < '{}' and
          lu.season > 1994

    ) as bm on ( (b.game_hash = bm.game_hash) and (b.team = bm.team) )

inner join team_advanced_boxscore_stats as adv on ( (bm.team = adv.team) and (bm.game_hash = adv.game_hash) )
inner join team_figure4_boxscore_stats as fig on ( (adv.team = fig.team) and (adv.game_hash = fig.game_hash) )
inner join team_misc_boxscore_stats as misc on ( (fig.team = misc.team) and (fig.game_hash = misc.game_hash) )
inner join team_scoring_boxscore_stats as score on ( (misc.team = score.team) and (misc.game_hash = score.game_hash) )
inner join team_traditional_boxscore_stats as trad on ( (score.team = trad.team) and (score.game_hash = trad.game_hash) )
