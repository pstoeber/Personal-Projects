select basic.player_id,
       box_view.team,
       box_view.game_hash,
       box_view.game_date,
       basic.pts,
       basic.minutes_played,
       basic.fga,
       basic.3pa,
       basic.orb,
       basic.trb,
       basic.ast,
       basic.stl,
       basic.blk,
       basic.tov,
       basic.pf,
       adv.turnover_pct,
       adv.usage_pct,
       a_stats.pace,
       a_stats.pie,
       adv.offensive_rating,
       adv.defensive_rating,
       p_score.pct_2pt_fga,
       p_score.pct_3pt_fga,
       p_score.pct_pts_fbps,
       p_score.pct_pts_ft,
       p_score.pct_pts_off_to,
       p_usg.pct_fga,
       p_usg.pct_3pa,
       p_usg.pct_fta,
       p_usg.pct_stl,
       reg_avg.fg_a as tot_fg_a,
       reg_avg.3p_a as tot_3p_a,
       reg_avg.ft_a as tot_ft_a,
       reg_avg.reb as tot_reb,
       reg_avg.ast as tot_ast,
       reg_avg.blk as tot_blk,
       reg_avg.stl as tot_stl,
       reg_avg.pf as tot_pf,
       reg_avg.`TO` as tot_to,
       opp_team_pts.opp_pts,
       opp_team_pts.diff,
       misc.FBPS,
       misc.second_chance_pts

from (

     select b.game_hash,
            b.home_team as team,
            b.game_date,
            team.team_id as away_id,
            lu.season
     from box_score_map as b
     inner join game_date_lookup as lu on b.game_date = lu.day
     inner join team_info as team on b.away_team = team.team
     where b.game_date < current_date and
           lu.season > 2001

     ) as box_view

inner join player_team_map as play_m on ( (box_view.team = play_m.team) and (
box_view.season = play_m.season) )
inner join basic_box_stats as basic on ( (box_view.game_hash = basic.game_hash) and (
play_m.player_id = basic.player_id) )
inner join advanced_box_stats as adv on ( (box_view.game_hash = adv.game_hash) and (
play_m.player_id = adv.player_id) )
inner join player_scoring_stats as p_score on ( (box_view.game_hash = p_score.game_hash) and (
play_m.player_id = p_score.player_id) )
inner join player_usage_stats as p_usg on ( (box_view.game_hash = p_usg.game_hash) and (
play_m.player_id = p_usg.player_id) )
inner join team_advanced_boxscore_stats as a_stats on ( (box_view.game_hash = a_stats.game_hash) and (box_view.team = a_stats.team) )
left outer join RegularSeasonAverages as reg_avg on ( (basic.player_id = reg_avg.player_id) and (box_view.season -1 = reg_avg.season) )
inner join points as opp_team_pts on box_view.away_id = opp_team_pts.team_id and box_view.season -1 = opp_team_pts.season
inner join team_misc_boxscore_stats as misc on ( (box_view.game_hash = misc.game_hash) and (box_view.team = misc.team) )
order by basic.player_id,
         box_view.game_date asc;
