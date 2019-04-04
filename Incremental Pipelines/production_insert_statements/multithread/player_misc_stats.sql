insert into nba_stats_prod.player_misc_stats(
  select b.game_hash,
         id.player_id,
         misc.team,
         misc.PTS_OFF_TO,
         misc.2nd_PTS,
         misc.FBPs,
         misc.PITP,
         misc.OppPTS_OFF_TO,
         misc.Opp2nd_PTS,
         misc.OppFBPs,
         misc.OppPITP,
         misc.BLK,
         misc.BLKA,
         misc.PF,
         misc.PFD
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

  inner join nba_stats.player_misc_stats as misc on id.team = misc.team and
                                                    id.NAME = misc.name and
                                                    id.game_date = misc.game_date);
