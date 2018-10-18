
set foreign_key_checks = 0;

drop table nba_stats_prod.3pt_pct;
drop table nba_stats_prod.fg_pct;
drop table nba_stats_prod.rebound_pct;
drop table nba_stats_prod.points;
drop table nba_stats_prod.turnovers;
drop table nba_stats_prod.player_info;
drop table nba_stats_prod.RegularSeasonMiscTotals;
drop table nba_stats_prod.RegularSeasonTotals;
drop table nba_stats_prod.RegularSeasonAverages;
drop table nba_stats_prod.team_info;
drop table nba_stats_prod.team_standings;
drop table nba_stats_prod.DATABASECHANGELOGLOCK;
drop table nba_stats_prod.DATABASECHANGELOG;
drop table nba_stats_prod.active_rosters;
drop table nba_stats_prod.box_score_map;
drop table nba_stats_prod.basic_box_stats;
drop table nba_stats_prod.advanced_box_stats;
drop table nba_stats_prod.DATABASECHANGELOG;
drop table nba_stats_prod.DATABASECHANGELOGLOCK;

set foreign_key_checks = 1;


/*
finding the team stats for a particular season where a team had at least
one player that scored equal to or more than the top 200 scorers during the years
2010 and greater

or

the team consistent of 5 or more shooters that shot over the league average of the top
200 shooters from 2010 to present and took at least 4 attempts per game while playing
equal to or more than the average games played dating back to 2000
 */

select stand.team,
       stand.conference,
       stand.season,
       stand.wins as wins,
       stand.loses as loses,
       stand.ppg,
       stand.opp_ppg,
       fg.own as fg_pct,
       fg.opp as opp_fg_pct,
       3pt.own as 3pt_pct,
       3pt.opp as opp_3pt_pct,
       3pt.ft_pct,
       reb.off as off_rebs,
       reb.def as def_rebs,
       reb.tot as tot_rebs,
       t_o.own as own_to,
       t_o.opp as forced_to
from team_standings as stand
inner join team_info as team on team.team = stand.team
inner join fg_pct as fg on ((team.team_id = fg.team_id) and (stand.season = fg.season))
inner join 3pt_pct as 3pt on ((team.team_id = 3pt.team_id) and (stand.season = 3pt.season))
inner join rebound_pct as reb on ((team.team_id = reb.team_id) and (stand.season = reb.season))
inner join turnovers as t_o on ((team.team_id = t_o.team_id) and (stand.season = t_o.season))
inner join (

    select team.team_id, crit.team, crit.season
    from (
        select team, season, player_id
        from RegularSeasonAverages
        where pts >= (select avg(a.pts)
                      from (
                        select pts from RegularSeasonAverages
                        where season >= 2010 and
                              gp >= (select avg(gp) from RegularSeasonAverages)
                        order by pts desc limit 150
                      ) as a) and
             season > 10
        ) as crit
        inner join team_standings as stand on ( (crit.team = stand.team) and (crit.season = stand.season) )
        inner join team_info as team on team.team = crit.team
        group by team.team_id,
                 crit.team,
                 crit.season
        having count(crit.player_id) >= 1

        union distinct

        select team.team_id, crit2.team, crit2.season
        from (
            select team, season, player_id
            from RegularSeasonAverages
            where 3p_pct >= (select avg(a.3p_pct)
                          from (
                            select 3p_pct
                            from RegularSeasonAverages
                            where season > 2010 and
                                  3p_a >= 4 and
                                  gp >= (select avg(gp) from RegularSeasonAverages)
                            order by 3p_pct desc limit 400
                          ) as a) and
                 season > 2010 and
                 3p_a >= 4
        ) as crit2
        inner join team_standings as stand on ((crit2.team = stand.team) and (crit2.season = stand.season))
        inner join team_info as team on team.team = crit2.team
        group by team.team_id,
                 crit2.team,
                 crit2.season
        having count(distinct crit2.player_id) > 2

    ) as team_crit on ((team_crit.team = stand.team) and (team_crit.season = stand.season))

where concat(stand.team, stand.season) in (

    select concat(team, season)
    from team_standings
    where conference like 'Western%' and
          wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Western%')

    union distinct

    select concat(team, season)
    from team_standings
    where conference like 'Eastern%' and
          wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Eastern%')
)

order by stand.season desc,
         stand.conference desc,
         stand.wins desc;


/* finding the individual players that meet the above criteria for the teams
extracted above
 */


select play.name,
       reg_a.season,
       reg_a.team,
       reg_a.gp,
       reg_a.gs as games_started,
       reg_a.min,
       reg_a.pts,
       reg_a.fg_a,
       reg_a.fg_m,
       reg_a.fg_pct,
       reg_a.3p_a,
       reg_a.3p_m,
       reg_a.3p_pct,
       reg_a.ft_a,
       reg_a.ft_m,
       reg_a.ft_pct,
       reg_a.or,
       reg_s.or as tot_off_rebs,
       reg_a.dr,
       reg_s.dr as tot_def_rebs,
       reg_a.reb,
       reg_s.reb as tot_rebs,
       reg_a.ast,
       reg_s.ast as tot_ast,
       reg_t.ast_to,
       reg_a.blk,
       reg_a.stl,
       reg_s.stl as tot_stl,
       reg_t.stl_to,
       reg_a.to,
       reg_s.to as tot_to,
       reg_t.dbldbl,
       reg_t.tridbl,
       reg_t.rat as rating,
       reg_t.sceff as scoring_eff,
       reg_t.sheff as shooting_eff
from player_info as play
inner join (

    select
      team.team_id, crit.team, crit.season, crit.player_id
    from (
       select team, season, player_id
       from RegularSeasonAverages
       where pts >= (select avg(a.pts)
                     from (
                            select pts
                            from RegularSeasonAverages
                            where season >= 2010 and
                                  gp >= (select avg(gp) from RegularSeasonAverages)
                            order by pts desc limit 150
                          ) as a) and
             season >= 2010
     ) as crit
    inner join team_standings as stand on ( (crit.team = stand.team) and (crit.season = stand.season) )
    inner join team_info as team on team.team = crit.team
    group by team.team_id,
             crit.team,
             crit.season,
             crit.player_id
    having count(crit.player_id) >= 1

    union distinct

    select team.team_id, crit2.team, crit2.season, crit2.player_id
    from (
       select team, season, player_id
       from RegularSeasonAverages
       where 3p_pct >= (select avg(a.3p_pct)
                       from (
                              select 3p_pct
                              from RegularSeasonAverages
                              where season > 2010 and
                                    3p_a >= 4 and
                                    gp >= (select avg(gp) from RegularSeasonAverages)
                              order by 3p_pct desc
                              limit 400
                            ) as a) and
             season > 2010
         ) as crit2
    inner join team_standings as stand on ( (crit2.team = stand.team) and (crit2.season = stand.season) )
    inner join team_info as team on team.team = crit2.team
    group by team.team_id,
             crit2.team,
             crit2.season,
             crit2.player_id
    having count(distinct crit2.player_id) > 2
    ) as criteria on (criteria.player_id = play.player_id)
inner join RegularSeasonAverages as reg_a on ((criteria.player_id = reg_a.player_id) and
   (criteria.team = reg_a.team) and
      (criteria.season = reg_a.season))
inner join RegularSeasonMiscTotals as reg_t on ((criteria.player_id = reg_t.player_id) and
   (criteria.team = reg_t.team) and
      (criteria.season = reg_t.season))
inner join RegularSeasonTotals as reg_s on ((criteria.player_id = reg_s.player_id) and
   (criteria.team = reg_s.team) and
      (criteria.season = reg_s.season))
inner join team_info as team on team.team_id = criteria.team_id

where reg_a.season >= 2010 and
      concat(team.team, reg_a.season) in (

            select concat(team, season)
            from team_standings
            where conference like 'Western%' and
                  wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Western%')

            union distinct

            select concat(team, season)
            from team_standings
            where conference like 'Eastern%' and
                  wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Eastern%')

      )
order by reg_a.season desc,
         reg_a.team desc,
         reg_a.pts desc;


/*
finding stats on teams over the last 8 seasons with the
following specs:

-- at least 3 players shooting over 38 percent with at least 4
attempts per game

and/or

-- at least 2 players with an above average ast/to and above average
stl/to and averages 7 ast or more and has played in more than the average
amount of games and above average minutes

and/or

-- 2 or more players that are rebounding 1.5 offensive rebound more than the
2018 league avg and average at least 8 rebounds a game.  For season played
from 2010 through the present
 */

select stand.team,
       stand.conference,
       stand.season,
       stand.wins,
       stand.loses,
       stand.ppg,
       stand.opp_ppg ,
       fg.own as fg_pct,
       fg.opp as opp_fg_pct,
       3pt.own as 3pt_pct,
       3pt.opp as opp_3pt_pct,
       3pt.ft_pct,
       reb.off as off_rebs,
       reb.def as def_rebs,
       reb.tot as tot_rebs,
       t_o.own as own_to,
       t_o.opp as forced_to
from team_standings as stand
inner join team_info as team on team.team = stand.team
inner join (

    select team.team_id, reg.team, reg.season
    from RegularSeasonAverages as reg
    inner join team_info as team on team.team = reg.team
    where reg.3p_pct >= (select avg(a.3p_pct)
                    from (
                          select 3p_pct
                          from RegularSeasonAverages
                          where season > 2010 and
                                3p_a >= 4 and
                                gp >= (select avg(gp) from RegularSeasonAverages)
                          order by 3p_pct desc limit 1500
                        ) as a) and
            reg.season > 2010 and
            reg.3p_a >= 4
    group by team.team_id,
             reg.team,
             reg.season
    having count(reg.player_id) > 2

    union distinct

    select team.team_id, reg_a.team, reg_a.season
    from RegularSeasonAverages as reg_a
    inner join team_info as team on team.team = reg_a.team
    inner join RegularSeasonMiscTotals as reg_m on ((reg_m.player_id = reg_a.player_id) and (reg_m.season = reg_a.season))
    where reg_a.ast >= 7 and
          reg_m.ast_to >= (select avg(ast_to) from RegularSeasonMiscTotals where season = 2018) and
          reg_m.stl_to >= (select avg(stl_to) from RegularSeasonMiscTotals where season = 2018) and
          reg_a.min >= (select avg(min) from RegularSeasonAverages where season = 2018) and
          reg_a.gp >= (select avg(gp) from RegularSeasonAverages where season = 2018) and
          reg_a.season >= 2010
    group by team.team_id,
             reg_a.team,
             reg_a.season
    having count(reg_a.player_id) >= 1

    union distinct

    select team.team_id, reg.team, reg.season
    from RegularSeasonAverages as reg
    inner join team_info as team on team.team = reg.team
    where reg.`or` >= (select avg(`or`)+ 1.5 from RegularSeasonAverages where season = 2018) and -- 2 and
          reg.reb >= 8 and
          reg.season >= 2010
    group by team.team_id,
             reg.team,
             reg.season
    having count(reg.player_id) > 1

    ) as criteria on ( (criteria.team = stand.team) and (criteria.season = stand.season) )

inner join fg_pct as fg on ( (criteria.team_id = fg.team_id) and (
    criteria.season = fg.season) )
inner join 3pt_pct as 3pt on ( (criteria.team_id = 3pt.team_id) and (
    criteria.season = 3pt.season) )
inner join rebound_pct as reb on ((criteria.team_id = reb.team_id) and (
    criteria.season = reb.season) )
inner join turnovers as t_o on ((criteria.team_id = t_o.team_id) and (
    criteria.season = t_o.season))

where concat(stand.team, stand.season) in (

    select concat(team, season)
    from team_standings
    where conference like 'Western%' and
          wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Western%')

    union distinct

    select concat(team, season)
    from team_standings
    where conference like 'Eastern%' and
          wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Eastern%')
)

order by stand.season desc,
         stand.conference desc,
         stand.wins desc;


/*
finding players used in the criteria of the above query to
select specific teams
*/


select play.name,
       play.player_id,
       reg_a.season,
       reg_a.team,
       reg_a.gp,
       reg_a.gs as games_started,
       reg_a.min,
       reg_a.pts,
       reg_a.fg_m,
       reg_a.fg_a,
       reg_a.fg_pct,
       reg_a.3p_m,
       reg_a.3p_a,
       reg_a.3p_pct,
       reg_a.ft_m,
       reg_a.ft_a,
       reg_a.ft_pct,
       reg_a.or,
       reg_s.or as tot_off_rebs,
       reg_a.dr,
       reg_s.dr as tot_def_rebs,
       reg_a.reb,
       reg_s.reb as tot_rebs,
       reg_a.ast,
       reg_s.ast as tot_ast,
       reg_t.ast_to,
       reg_a.blk,
       reg_a.stl,
       reg_s.stl as tot_stl,
       reg_t.stl_to,
       reg_a.to,
       reg_s.to as tot_to,
       reg_t.dbldbl,
       reg_t.tridbl,
       reg_t.rat as rating,
       reg_t.sceff as scoring_eff,
       reg_t.sheff as shooting_eff
from player_info as play
inner join (

    select team.team_id, a.team, a.season, reg_a.player_id
    from (
           select reg.team, reg.season -- , reg.player_id
           from RegularSeasonAverages as reg
           inner join team_info as team on team.team = reg.team
           where reg.3p_pct >= .33 and
                 reg.season > 2010 and
                 reg.3p_a >= 4
           group by reg.team,
                    reg.season
           having count(reg.player_id) > 3
         ) as a
    inner join team_info as team on team.team = a.team
    inner join RegularSeasonAverages as reg_a on ((reg_a.team = a.team) and (
      reg_a.season = a.season))
    where reg_a.3p_pct >= .33 and
          reg_a.season > 2010 and
          reg_a.3p_a >= 4

    union distinct

    select team.team_id, b.team, b.season, reg.player_id
    from (
           select reg_a.team, reg_a.season
           from RegularSeasonAverages as reg_a
           inner join RegularSeasonMiscTotals as reg_m on ((reg_m.player_id = reg_a.player_id) and (
               reg_m.season = reg_a.season))
           where reg_a.ast >= 6 and
                 reg_m.stl_to >= (select avg(stl_to) from RegularSeasonMiscTotals where season = 2018) and
                 reg_a.min >= (select avg(min) from RegularSeasonAverages where season = 2018) and
                 reg_a.gp >= (select avg(gp) from RegularSeasonAverages where season = 2018) and
                 reg_a.season >= 2010
           group by reg_a.team,
                    reg_a.season
           having count(reg_a.player_id) >= 1
         ) as b
    inner join team_info as team on team.team = b.team
    inner join RegularSeasonAverages as reg on ((reg.team = b.team) and (
        reg.season = b.season))
    inner join RegularSeasonMiscTotals as reg_m on ((reg_m.team = b.team) and (
        reg_m.season = b.season))
    where reg.ast >= 6 and
          reg_m.stl_to >= (select avg(stl_to) from RegularSeasonMiscTotals where season = 2018) and
          reg.min >= (select avg(min) from RegularSeasonAverages where season = 2018) and
          reg.gp >= (select avg(gp) from RegularSeasonAverages where season = 2018) and
          reg.season >= 2010

    union distinct

    select team.team_id, c.team, c.season, reg_a.player_id
    from (
           select reg.team, reg.season
           from RegularSeasonAverages as reg
           where reg.`or` >= (select avg(`or`) + 1.5 from RegularSeasonAverages where season = 2018) and -- 2 and
                 reg.reb >= 7 and
                 reg.season >= 2010
           group by reg.team,
                    reg.season
           having count(reg.player_id) > 1
         ) as c
    inner join team_info as team on team.team = c.team
    inner join RegularSeasonAverages as reg_a on ((reg_a.team = c.team) and (
        reg_a.season = c.season))
    where reg_a.`or` >= (select avg(`or`) + 1.5 from RegularSeasonAverages where season = 2018) and -- 2 and
          reg_a.reb >= 7 and
          reg_a.season >= 2010
    ) as criteria on criteria.player_id = play.player_id
inner join RegularSeasonAverages as reg_a on ((reg_a.player_id = criteria.player_id) and (
    reg_a.season = criteria.season) and (
      reg_a.team = criteria.team))
inner join RegularSeasonMiscTotals as reg_t on ((reg_t.player_id = criteria.player_id) and (
    reg_t.season = criteria.season) and (
      reg_t.team = criteria.team))
inner join RegularSeasonTotals as reg_s on ((reg_s.player_id = criteria.player_id) and (
    reg_s.season = criteria.season) and (
      reg_s.team = criteria.team))
inner join team_info as team on team.team_id = criteria.team_id

where reg_a.season >= 2010 and
      concat(team.team, reg_a.season) in (

            select distinct concat(team, season)
            from team_standings
            where conference like 'Western%' and
                  wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Western%') and
                  season >= 2010

            union all

            select distinct concat(team, season)
            from team_standings
            where conference like 'Eastern%' and
                  wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Eastern%') and
                  season >= 2010
      ) and
      reg_a.gp >= (select avg(gp) from RegularSeasonAverages where season = 2018)
order by reg_a.season desc,
         reg_a.team desc,
         reg_a.pts desc;



/*
Showing teams that have average more rebounds: offensive, defensive and total rebounds

and

teams that average less turnovers than the league average since 2013 and create more turnovers
than the league average since 2013

and

and teams that shot free-throws at a higher percentage than the league average since 2013

all factors must be met within the same season in order for a team to qualify
*/

select stand.team,
       stand.conference,
       stand.season,
       stand.wins,
       stand.loses,
       stand.ppg,
       stand.opp_ppg ,
       fg.own as fg_pct,
       fg.opp as opp_fg_pct,
       3pt.own as 3pt_pct,
       3pt.opp as opp_3pt_pct,
       3pt.ft_pct,
       reb.off as off_rebs,
       reb.def as def_rebs,
       reb.tot as tot_rebs,
       t_o.own as own_to,
       t_o.opp as forced_to
from team_standings as stand
left join team_info as team on stand.team = team.team
inner join (

    select stats.team_id, stats.season
    from (

      select team_id, season
      from rebound_pct
      where off >= (select avg(off) from rebound_pct where season >= 2013) and
            def >= (select avg(def) from rebound_pct where season >= 2013) and
            tot >= (select avg(tot) from rebound_pct where season >= 2013) and
            season >= 2010

      union all

      select team_id, season
      from turnovers
      where own <= (select avg(own) from turnovers where season >= 2013) and
            opp >= (select avg(opp) from turnovers where season >= 2013) and
            season >= 2010

      union all

      select team_id, season
      from 3pt_pct
      where ft_pct >= (select avg(ft_pct) from 3pt_pct where season >= 2013) and
            season >= 2010
    ) as stats
    group by stats.team_id,
             stats.season
    having count(concat(stats.team_id, stats.season)) >= 2
) as criteria on ((criteria.team_id = team.team_id) and (
    criteria.season = stand.season))

inner join fg_pct as fg on ((criteria.team_id = fg.team_id) and (
    criteria.season = fg.season))
inner join 3pt_pct as 3pt on ((criteria.team_id = 3pt.team_id) and (
    criteria.season = 3pt.season))
inner join rebound_pct as reb on ((criteria.team_id = reb.team_id) and (
    criteria.season = reb.season))
inner join turnovers as t_o on ((criteria.team_id = t_o.team_id) and (
    criteria.season = t_o.season))

order by stand.season desc,
         stand.conference desc,
         stand.wins desc;


/*
finding teams with winning road records and observing the
stats of players in individual road games
*/


select play.player_id,
       act.team,
       act.team_id,
       map.home_away,
       map.game_hash,
       map.game_date,
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
       basic.pts
from active_rosters as act
inner join player_info as play on (act.player_id = play.player_id)
inner join box_scores_map_view as map on ( (act.team = map.team) and (
        map.game_date >= '2017-8-30') )

inner join basic_box_stats as basic on ( (act.player_id = basic.player_id) and (
        map.game_hash = basic.game_hash) )
order by map.game_hash,
         act.team


select play.player_id,
       play.name,
       p_t_map.team,
       p_t_map.season,
       box_view.game_hash,
       box_view.game_date


from player_info as play
inner join player_team_map as p_t_map on (play.player_id = p_t_map.player_id)
inner join (

    select a.team, a.season
    from (
          select team, season
          from team_standings
          where wins >= (select avg(wins) from team_standings where season >= 2014 and conference like 'Western%') and
                season >= 2014

          union all

          select team, season
          from team_standings
          where wins >= (select avg(wins) from team_standings where season >= 2014 and conference like 'Eastern%') and
                season >= 2014
        ) as a
    ) as team_criteria on ( (p_t_map.team = team_criteria.team) and (
        p_t_map.season = team_criteria.season) )

inner join box_scores_map_view as box_view on ( (p_t_map.team = box_view.team) and (
        p_t_map.season = year(box_view.game_date)) )
inner join basic_box_stats as basic on ( (box_view.game_hash = basic.game_hash) and(
        p_t_map.player_id = basic.player_id) )
inner join advanced_box_stats advanced on ( (p_t_map.player_id = advanced.player_id) and (
        box_view.game_hash = advanced.game_hash) )






select distinct reg.player_id, play.name, team.team_id, reg.team, stand.conference
from RegularSeasonAverages as reg
       inner join player_info as play on play.player_id = reg.player_id
       inner join team_info as team on (team.team = reg.team)
       inner join team_standings as stand on ((team.team = stand.team) and (stand.season = reg.season))
where stand.season = (select max(season) from team_standings)




select *
from nba_stats_prod.advanced_box_stats as a
inner join nba_stats_prod.basic_box_stats as b on ( (a.game_hash = b.game_hash) and (a.player_id = b.player_id) )

select *
from nba_stats_prod.box_scores_map_view as a
inner join nba_stats_prod.advanced_box_stats as m on (a.game_hash = m.game_hash)
inner join nba_stats_prod.active_rosters as r on ( (m.player_id = r.player_id) and (a.team = r.team) )
where a.game_date > '2017-8-30'

select count(distinct a.game_hash)
from (select play.name,
             p_map.player_id,
             box.game_hash,
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
             basic.pts,
             basic.plus_minus
      from player_team_map as p_map
             inner join player_info as play on play.player_id = p_map.player_id
             inner join box_scores_map_view as box on ((box.team = p_map.team) and (
              year(box.game_date) in (2017, 2018)
              ))
             inner join basic_box_stats as basic on ((play.player_id = basic.player_id) and (
              basic.game_hash = box.game_hash
              ))
      where p_map.season = 2018
        and p_map.team like '%Wizards%') as a




select stand.team,
       stand.conference,
       stand.season,
       floor(stand.wins) as wins,
       stand.loses as loses,
       stand.ppg,
       stand.opp_ppg,
       fg.own as fg_pct,
       fg.opp as opp_fg_pct,
       3pt.own as 3pt_pct,
       3pt.opp as opp_3pt_pct,
       3pt.ft_pct,
       reb.off as off_rebs,
       reb.def as def_rebs,
       reb.tot as tot_rebs,
       t_o.own as own_to,
       t_o.opp as forced_to
from team_standings as stand
inner join team_info as team on team.team = stand.team
inner join fg_pct as fg on ((team.team_id = fg.team_id) and (stand.season = fg.season))
inner join 3pt_pct as 3pt on ((team.team_id = 3pt.team_id) and (stand.season = 3pt.season))
inner join rebound_pct as reb on ((team.team_id = reb.team_id) and (stand.season = reb.season))
inner join turnovers as t_o on ((team.team_id = t_o.team_id) and (stand.season = t_o.season))
inner join (

    select team.team_id, crit.team, crit.season
    from (
        select team, season, player_id
        from RegularSeasonAverages
        where pts >= (select avg(a.pts)
                      from (
                        select pts from RegularSeasonAverages
                        where season >= 2010 and
                              gp >= (select avg(gp) from RegularSeasonAverages)
                        order by pts desc limit 150
                      ) as a) and
             season > 10
        ) as crit
        inner join team_standings as stand on ( (crit.team = stand.team) and (crit.season = stand.season) )
        inner join team_info as team on team.team = crit.team
        group by team.team_id,
                 crit.team,
                 crit.season
        having count(crit.player_id) >= 1

        union distinct

        select team.team_id, crit2.team, crit2.season
        from (
            select team, season, player_id
            from RegularSeasonAverages
            where 3p_pct >= (select avg(a.3p_pct)
                          from (
                            select 3p_pct
                            from RegularSeasonAverages
                            where season > 2010 and
                                  3p_a >= 4 and
                                  gp >= (select avg(gp) from RegularSeasonAverages)
                            order by 3p_pct desc limit 400
                          ) as a) and
                 season > 2010 and
                 3p_a >= 4
        ) as crit2
        inner join team_standings as stand on ((crit2.team = stand.team) and (crit2.season = stand.season))
        inner join team_info as team on team.team = crit2.team
        group by team.team_id,
                 crit2.team,
                 crit2.season
        having count(distinct crit2.player_id) > 2

    ) as team_crit on ((team_crit.team = stand.team) and (team_crit.season = stand.season))

where concat(stand.team, stand.season) in (

    select concat(team, season)
    from team_standings
    where conference like 'Western%' and
          wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Western%')

    union distinct

    select concat(team, season)
    from team_standings
    where conference like 'Eastern%' and
          wins >= (select avg(wins) from team_standings where season >= 2010 and conference like 'Eastern%')
)

order by stand.season desc,
         stand.conference desc,
         stand.wins desc


select basic.player_id,
       basic.game_hash,
       basic.pts,
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
       adv.defensive_rating
from player_team_map as p_map
inner join box_scores_map_view as box on  ( (p_map.team = box.team) and (year(box.game_date) = p_map.season) )
inner join  basic_box_stats as basic on ( (p_map.player_id = basic.player_id) and (
        box.game_hash = basic.game_hash) )
inner join  advanced_box_stats as adv on ( (p_map.player_id = adv.player_id) and (
        box.game_hash = adv.game_hash) )




select adv.player_id,
       adv.game_hash,
       basic.pts,
       adv.minutes_played,
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
       adv.defensive_rating
from player_team_map as p_map
inner join box_scores_map_view as box on  ( (p_map.team = box.team) and (year(box.game_date) = p_map.season) )
inner join  advanced_box_stats as adv on ( (p_map.player_id = adv.player_id) and (
        box.game_hash = adv.game_hash) )
inner join  basic_box_stats as basic on ( (p_map.player_id = basic.player_id) and (
        box.game_hash = basic.game_hash) )


select * from basic_box_stats where player_id = 2030190
select * from box_scores_map_view
-- creating team_misc_boxscore_stats_view

create or replace view nba_stats.team_misc_boxscore_stats_view as
  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.pts_off_to,
         a.2nd_pts,
         a.fbps,
         a.PITP,
         a.opp_pts_off_to,
         a.opp_2nd_pts,
         a.opp_fbps,
         a.opp_pitp
  from box_score_map_view as b_map
  inner join team_misc_boxscore_stats as a on ( (b_map.home_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) )

  union

  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.pts_off_to,
         a.2nd_pts,
         a.fbps,
         a.PITP,
         a.opp_pts_off_to,
         a.opp_2nd_pts,
         a.opp_fbps,
         a.opp_pitp
  from box_score_map_view as b_map
  inner join team_misc_boxscore_stats as a on ( (b_map.away_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) );



-- creating team_box_score_advanced_stats view

create or replace view nba_stats.advanced_team_boxscore_stats_view as
  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.OFFRTG,
         a.DEFRTG,
         a.NETRTG,
         a.`AST%`,
         a.`AST/TO`,
         a.ast_ratio,
         a.`OREB%`,
         a.`DREB%`,
         a.`REB%`,
         a.`TOV%`,
         a.`EFG%`,
         a.`TS%`,
         a.pace,
         a.PIE
  from box_score_map_view as b_map
  inner join advanced_team_boxscore_stats as a on ( (b_map.home_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) )

  union

  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.OFFRTG,
         a.DEFRTG,
         a.NETRTG,
         a.`AST%`,
         a.`AST/TO`,
         a.ast_ratio,
         a.`OREB%`,
         a.`DREB%`,
         a.`REB%`,
         a.`TOV%`,
         a.`EFG%`,
         a.`TS%`,
         a.pace,
         a.PIE
  from box_score_map_view as b_map
  inner join advanced_team_boxscore_stats as a on ( (b_map.away_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) );


-- creating figure4_team_box_score_stats_view

create or replace view nba_stats.figure4_team_boxscore_stats_view as
  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         cast(substring(a.`EFG%`, 1, 4) as decimal(10,1)),
         a.fta_rate,
         a.`TOV%`,
         cast(substring(a.`EFG%`, 1, 4) as decimal(10,1)),
         cast(substring(a.`opp_efg%`, 1, 4) as decimal(10,1)),
         a.opp_fta_rate,
         a.`opp_tov%`,
         cast(substring(a.`OPP_OREB%`, 1, 4) as decimal(10,1))
  from box_score_map_view as b_map
  inner join figure4_team_boxscore_stats as a on ( (b_map.home_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) )

  union

  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         cast(substring(a.`EFG%`, 1, 4) as decimal(10,1)),
         a.fta_rate,
         a.`TOV%`,
         cast(substring(a.`EFG%`, 1, 4) as decimal(10,1)),
         cast(substring(a.`opp_efg%`, 1, 4) as decimal(10,1)),
         a.opp_fta_rate,
         a.`opp_tov%`,
         cast(substring(a.`OPP_OREB%`, 1, 4) as decimal(10,1))
  from box_score_map_view as b_map
  inner join figure4_team_boxscore_stats as a on ( (b_map.away_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) );

-- creating misc_team_boxscore_stats_view

create or replace view nba_stats.team_misc_boxscore_stats_view as
mi
-- creating team_scoring_boxscore_stats_view

create or replace view nba_stats.team_scoring_boxscore_stats_view as
  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.`%fga_2pt`,
         a.`%FGA_3PT`,
         a.`%PTS_2PT`,
         a.`%PTS_2PT_MR`,
         a.`%PTS_3PT`,
         a.`%PTS_FBPS`,
         a.`%PTS_FT`,
         a.`%PTS_OFF_TO`,
         a.`%PTS_PITP`,
         a.`2FGM_%AST`,
         a.`2FGM_%UAST`,
         a.`3FGM_%AST`,
         a.`3FGM_%UAST`,
         a.`FGM_%AST`,
         a.`FGM_%UAST`
  from box_score_map_view as b_map
  inner join team_scoring_boxscore_stats as a on ( (b_map.home_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) )

  union

  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.`%fga_2pt`,
         a.`%FGA_3PT`,
         a.`%PTS_2PT`,
         a.`%PTS_2PT_MR`,
         a.`%PTS_3PT`,
         a.`%PTS_FBPS`,
         a.`%PTS_FT`,
         a.`%PTS_OFF_TO`,
         a.`%PTS_PITP`,
         a.`2FGM_%AST`,
         a.`2FGM_%UAST`,
         a.`3FGM_%AST`,
         a.`3FGM_%UAST`,
         a.`FGM_%AST`,
         a.`FGM_%UAST`
  from box_score_map_view as b_map
  inner join team_scoring_boxscore_stats as a on ( (b_map.away_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) );

-- creating traditional_team_boxscore_stats_view

create or replace view nba_stats.traditional_team_boxscore_stats_view as
  select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.pts,
         a.fgm,
         a.fga,
         a.`FG%`,
         a.3pm,
         a.3pa,
         a.`3P%`,
         a.ftm,
         a.fta,
         a.`FT%`,
         a.oreb,
         a.dreb,
         a.reb,
         a.ast,
         a.tov,
         a.stl,
         a.blk,
         a.pf,
         a.`+/-`
  from box_score_map_view as b_map
  inner join traditional_team_boxscore_stats as a on ( (b_map.home_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) )

  union

    select b_map.game_hash,
         a.team,
         a.`W/L`,
         a.min,
         a.pts,
         a.fgm,
         a.fga,
         a.`FG%`,
         a.3pm,
         a.3pa,
         a.`3P%`,
         a.ftm,
         a.fta,
         a.`FT%`,
         a.oreb,
         a.dreb,
         a.reb,
         a.ast,
         a.tov,
         a.stl,
         a.blk,
         a.pf,
         a.`+/-`
  from box_score_map_view as b_map
  inner join traditional_team_boxscore_stats as a on ( (b_map.away_team = a.team) and (b_map.game_date = str_to_date(a.GAME_DATE, '%m/%d/%Y')) );
