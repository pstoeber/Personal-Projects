select basic.player_id,
       play.name,
       bm.target as team,
       bm.game_hash,
       bm.game_date,
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

from (select gm.game_hash, gm.target, gm.opp, gm.game_date, gm.season, t.team_id as opp_id

      from (select m.game_hash, m.home_team as target, m.away_team as opp, m.game_date, lu.season
            from box_score_map as m
                   inner join game_date_lookup as lu on m.game_date = lu.day
            where m.home_team like '{}%'

            union

            select m.game_hash, m.away_team as target, m.home_team as opp, m.game_date, lu.season
            from box_score_map as m
                   inner join game_date_lookup as lu on m.game_date = lu.day
            where m.away_team like '{}%') as gm
             inner join team_info as t on gm.opp = t.team
      order by game_date desc
      limit 9) as bm
inner join team_info as t_opp on bm.opp = t_opp.team

inner join (

          select name, team, player_id
          from active_rosters
          where player_id not in (select player_id from injured_players) and
                team like '{}%'

          ) as player on bm.target = player.team

inner join basic_box_stats as basic on ( (bm.game_hash = basic.game_hash) and (
player.player_id = basic.player_id) )
inner join advanced_box_stats as adv on ( (bm.game_hash = adv.game_hash) and (
player.player_id = adv.player_id) )
inner join player_scoring_stats as p_score on ( (bm.game_hash = p_score.game_hash) and (
player.player_id = p_score.player_id) )
inner join player_usage_stats as p_usg on ( (bm.game_hash = p_usg.game_hash) and (
player.player_id = p_usg.player_id) )
inner join team_advanced_boxscore_stats as a_stats on ( (bm.game_hash = a_stats.game_hash) and (bm.target = a_stats.team) )
left outer join RegularSeasonAverages as reg_avg on ( (basic.player_id = reg_avg.player_id) and (bm.season -1 = reg_avg.season) )
inner join points as opp_team_pts on bm.opp_id = opp_team_pts.team_id and bm.season -1 = opp_team_pts.season
inner join team_misc_boxscore_stats as misc on ( (bm.game_hash = misc.game_hash) and (bm.target = misc.team) )
inner join player_info as play on player.player_id = play.player_id
order by basic.player_id,
         bm.game_date asc;
