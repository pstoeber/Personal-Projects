select play.player_id,
       box_view.team,
       box_view.game_hash,
       box_view.game_date,
       box_view.home_away,
       basic.minutes_played,
       basic.fg,
       basic.fga,
       basic.fg_pct,
       basic.3p,
       basic.3pa,
       basic.3p_pct,
       basic.ft,
       basic.ft_pct,
       basic.orb,
       basic.drb,
       basic.trb,
       basic.ast,
       basic.stl,
       basic.blk,
       basic.tov,
       basic.pf,
       adv.true_shooting_pct,
       adv.effective_fg_pct,
       adv.3P_attempt_rate,
       adv.FT_attempt_rate,
       adv.offensive_reb_rate,
       adv.defensive_reb_rate,
       adv.total_reb_pct,
       adv.assist_pct,
       adv.steal_pct,
       adv.block_pct,
       adv.turnover_pct,
       adv.usage_pct,
       adv.offensive_rating,
       adv.defensive_rating,
       basic.pts
from (

    select game_hash, team, game_date, home_away
    from box_scores_map_view
    where game_date < '2018-03-13'
    order by game_date desc

         ) as box_view
inner join game_date_lookup as lu on box_view.game_date = lu.day
inner join player_team_map as play_m on ( (box_view.team = play_m.team) and (
        lu.season = play_m.season) )
inner join basic_box_stats as basic on ( (box_view.game_hash = basic.game_hash) and (
        play_m.player_id = basic.player_id) )
inner join advanced_box_stats as adv on ( (box_view.game_hash = adv.game_hash) and (
        play_m.player_id = adv.player_id) )
inner join player_info as play on play_m.player_id = play.player_id
where lu.season > 1994
order by box_view.game_date desc;
