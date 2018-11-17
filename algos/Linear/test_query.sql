select play.name,
                          player.player_id,
                          bm.team,
                          bm.home_away,
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
                   where team like 'Washington%' /*and
                         game_date = (select max(game_date) from box_scores_map_view where team like 'Washington%')*/
                   order by game_date desc limit 13

                        ) as bm
                   inner join game_date_lookup as lu on bm.game_date = lu.day

                   inner join (

                        select map.player_id, map.team
                        from player_team_map as map
                        inner join player_info as play on map.player_id = play.player_id
                        where play.name in ('{}') and
                              map.season = 2018

                   ) as player on bm.team = player.team

                  inner join basic_box_stats as basic on ( (bm.game_hash = basic.game_hash) and (player.player_id = basic.player_id) )
                  inner join advanced_box_stats as adv on ( (bm.game_hash = adv.game_hash) and (player.player_id = adv.player_id) )
                  inner join player_info as play on player.player_id = play.player_id
                  where bm.team like 'Washington%' and
                        lu.season = 2018 and
                        basic.minutes_played not like '00:00:00';
