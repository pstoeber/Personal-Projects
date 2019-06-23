select b.game_hash,
       id.player_id,
       t.team_id,
       misc.PTS_OFF_TO as pts_off_to,
       misc.2nd_PTS as second_chance_pts,
       misc.FBPs as fbps,
       misc.PITP as pitp,
       misc.OppPTS_OFF_TO as opp_pts_off_to,
       misc.Opp2nd_PTS as opp_second_chance_pts,
       misc.OppFBPs as opp_fbps,
       misc.OppPITP as opp_pitp,
       misc.BLK as blk,
       misc.BLKA as blka,
       misc.PF as pf,
       misc.PFD as pfd,
       misc.source_link,
       misc.created_at
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

inner join player_misc_stats as misc on id.team = misc.team and
                                                  id.NAME = misc.name and
                                                  id.game_date = misc.game_date
inner join team_info as t on misc.team = t.team
