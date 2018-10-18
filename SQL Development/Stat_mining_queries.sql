


 /*
set foreign_key_checks = 0

drop table nba_stats_prod.3p_pct;
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

set foreign_key_checks = 1
*/

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





select 3pt_pct.team_id, 3pt_pct.year, 3pt_pct.own as own_3pt_pct , fg_pct.own as own_fg_pct, points.own as own_pts from 3pt_pct inner join FG_PCT on FG_PCT.team_id = 3PT_PCT.team_id and FG_PCT.year = 3PT_PCT.year inner join points on points.team_id = FG_PCT.team_id and points.year = FG_PCT.year group by 3pt_pct.team_id, 3pt_pct.year, 3pt_pct.own , fg_pct.own, points.own having 3pt_pct.own > (select avg(own) from 3pt_pct) and fg_pct.own > (select avg(own) from fg_pct) and points.own > (select avg(own) from points)


-- finding teams that have above average 3pt, fg and reb pct above average and that first better than 15 games back
-- and had more wins than average scored more points than average and allowed below the league average on defense

select team.team,
       team.conference,
       team.season,
       team.GB as Games_back,
       team.w as win,
       team.l as loss,
       team.ppg,
       data_set.own_3pt_pct,
       data_set.own_fg_pct,
       data_set.off as off_reb,
       data_set.def as def_reb,
       data_set.tot as tot_reb
from (
      select 3pt_pct.team_id,
             3pt_pct.year,
             3pt_pct.own as own_3pt_pct ,
             fg_pct.own as own_fg_pct,
             points.own as own_pts,
             reb.off,
             reb.def,
             reb.tot
             from 3pt_pct
             inner join FG_PCT on FG_PCT.team_id = 3PT_PCT.team_id and FG_PCT.year = 3PT_PCT.year
             inner join points on points.team_id = FG_PCT.team_id and points.year = FG_PCT.year
             inner join rebound_pct as reb on reb.team_id = FG_PCT.team_id and reb.year = 3PT_PCT.year
             group by 3pt_pct.team_id,
                      3pt_pct.year,
                      3pt_pct.own,
                      fg_pct.own,
                      points.own,
                      reb.off,
                      reb.def,
                      reb.tot
            having 3pt_pct.own > (select avg(own) from 3pt_pct) and
                   fg_pct.own > (select avg(own) from fg_pct) and
                   points.own > (select avg(own) from points)
      ) as data_set
  inner join team_info on team_info.team_id = data_set.team_id
  inner join team_standings as team on team.team = team_info.team and team.season = data_set.year
  where team.GB <= 15
  group by team.team,
           team.season,
           team.conference,
           team.GB,
           team.w,
           team.l,
           team.ppg,
           team.opp_ppg,
           data_set.own_3pt_pct,
           data_set.own_fg_pct,
           data_set.off,
           data_set.def,
           data_set.tot
  having team.w > (select avg(w) from team_standings) and
         team.ppg > (select avg(ppg) from team_standings) and
         team.OPP_PPG < (select avg(opp_ppg) from team_standings)
  order by team.conference desc,
           team.season desc,
           team.gb asc;


-- finding individual player stats on the teams in above query

select distinct player_info.name,
                reg_avg.season,
                reg_avg.`FGM-A`,
                reg_avg.`FG%`,
                reg_avg.`3PM-A`,
                reg_avg.`3P%`,
                reg_avg.PTS,
                reg_avg.`OR`,
                reg_avg.DR,
                reg_avg.REB,
                misc_totals.`AST/TO`,
                misc_totals.`stl/to`
from (
       select team.team,
              team.conference,
              team.season,
              team.GB as Games_back,
              team.w as win,
              team.l as loss,
              team.ppg,
              data_set.own_3pt_pct,
              data_set.own_fg_pct,
              data_set.off as off_reb,
              data_set.def as def_reb,
              data_set.tot as tot_reb
       from (
            select 3pt_pct.team_id,
                   3pt_pct.year,
                   3pt_pct.own as own_3pt_pct ,
                   fg_pct.own as own_fg_pct,
                   points.own as own_pts,
                   reb.off,
                   reb.def,
                   reb.tot
                   from 3pt_pct
                   inner join FG_PCT on FG_PCT.team_id = 3PT_PCT.team_id and FG_PCT.year = 3PT_PCT.year
                   inner join points on points.team_id = FG_PCT.team_id and points.year = FG_PCT.year
                   inner join rebound_pct as reb on reb.team_id = FG_PCT.team_id and reb.year = 3PT_PCT.year
                   group by 3pt_pct.team_id,
                            3pt_pct.year,
                            3pt_pct.own,
                            fg_pct.own,
                            points.own,
                            reb.off,
                            reb.def,
                            reb.tot
                  having 3pt_pct.own >= (select avg(own) from 3pt_pct) or
                         fg_pct.own >= (select avg(own) from fg_pct) and
                         points.own >= (select avg(own) from points)
            ) as data_set
            inner join team_info on team_info.team_id = data_set.team_id
            inner join team_standings as team on team.team = team_info.team and team.season = data_set.year
            where team.GB <= 15
            group by team.team,
                     team.season,
                     team.conference,
                     team.GB,
                     team.w,
                     team.l,
                     team.ppg,
                     team.opp_ppg,
                     data_set.own_3pt_pct,
                     data_set.own_fg_pct,
                     data_set.off,
                     data_set.def,
                     data_set.tot
            having team.w >= (select avg(w) from team_standings) and
                   team.ppg >= (select avg(ppg) from team_standings) and
                   team.OPP_PPG <= (select avg(opp_ppg) from team_standings)
            order by team.conference desc,
                     team.season desc,
                     team.gb asc
             ) as team_set
inner join player_info on player_info.current_team = team_set.team
inner join regular_season_averages as reg_avg on reg_avg.player_id = player_info.player_id
inner join regular_season_misc_totals as misc_totals on misc_totals.name = player_info.name and misc_totals.season = reg_avg.season
where (misc_totals.`ast/to` >= (select avg(`ast/to`) from regular_season_misc_totals) or
       misc_totals.`stl/to` >= (select avg(`stl/to`) from regular_season_misc_totals)) and
      (misc_totals.sceff >= (select avg(sceff) from regular_season_misc_totals) or
       misc_totals.sheff >= (select avg(sheff) from regular_season_misc_totals) or
       misc_totals.rat >= (select avg(rat) from regular_season_misc_totals)) and
       substring(misc_totals.season, 6, 2) >= 13
group by player_info.name,
         reg_avg.season,
         reg_avg.`FGM-A`,
         reg_avg.`FG%`,
         reg_avg.`3PM-A`,
         reg_avg.`3P%`,
         reg_avg.PTS,
         reg_avg.`OR`,
         reg_avg.DR,
         reg_avg.REB,
         misc_totals.`ast/to`,
         misc_totals.`stl/to`
having reg_avg.`3P%` >= (select avg(`3P%`) from regular_season_averages) or
       reg_avg.PTS > (select avg(pts) from regular_season_averages);



-- Finding average stats of winning teams from the last 5 years

select distinct team_standings.team,
                team_standings.season,
                team_standings.w as wins,
                team_standings.l as loses,
                avg(data_set.avg_fg) as avg_fg_pct,
                avg(data_set.avg_3p) as avg_3p_pct,
                avg(data_set.avg_sceff) as avg_scoring_eff,
                avg(data_set.avg_sheff) as avg_shooting_eff,
                avg(data_set.avg_rat) as avg_rating,
                avg(data_set.avg_ast) as avg_ast,
                avg(data_set.avg_stl) as avg_stl,
                avg(data_set.avg_to) as avg_to,
                avg(data_set.avg_or) as avg_or,
                avg(data_set.avg_dr) as avg_dr,
                avg(data_set.avg_ast_to) as avg_ast_to,
                avg(data_set.avg_stl_ast) as avg_stl_ast
from (
    select player_info.player_id,
           reg_avg.season,
           reg_tot.team,
           avg(reg_avg.`FG%`) as avg_fg,
           avg(reg_avg.`3P%`) as avg_3p,
           avg(reg_avg.ast) as avg_ast,
           avg(reg_avg.stl) as avg_stl,
           avg(reg_avg.to) as avg_to,
           avg(reg_avg.`OR`) as avg_or,
           avg(reg_avg.DR) as avg_dr,
           avg(reg_avg.reb) as avg_reb,
           avg(reg_tot.DBLDBL) as avg_dbldbl,
           avg(reg_tot.TRIDBL) as avg_tridbl,
           avg(reg_tot.`AST/TO`) as avg_ast_to,
           avg(reg_tot.`STL/TO`) as avg_stl_ast,
           avg(reg_tot.SCEFF) as avg_sceff,
           avg(reg_tot.SHEFF) as avg_sheff,
           avg(reg_tot.RAT) as avg_rat
    from RegularSeasonAverages as reg_avg
    inner join RegularSeasonMiscTotals as reg_tot on reg_tot.player_id = reg_avg.player_id and reg_tot.season = reg_avg.season and reg_tot.team = reg_avg.team
    inner join player_info on player_info.player_id = reg_tot.player_id
    group by player_info.player_id,
             reg_avg.season,
             reg_tot.team,
             reg_avg.`FG%`,
             reg_avg.`3P%`,
             reg_avg.ast,
             reg_avg.stl,
             reg_avg.to,
             reg_avg.`OR`,
             reg_avg.DR,
             reg_avg.reb,
             reg_tot.DBLDBL,
             reg_tot.TRIDBL,
             reg_tot.`AST/TO`,
             reg_tot.`STL/TO`,
             reg_tot.SCEFF,
             reg_tot.SHEFF,
             reg_tot.RAT
      ) as data_set
inner join team_standings on data_set.team = team_standings.team and substring(data_set.season, 6, 2) = substring(team_standings.season, 3, 2)
where (data_set.season like "'17-'18" or
       data_set.season like "'16-'17" or
       data_set.season like "'15-'16" or
       data_set.season like "'14-'15" or
       data_set.season like "'13-'14") and
       team_standings.w >= (select avg(w) from team_standings)
group by team_standings.team,
         team_standings.season,
         team_standings.w,
         team_standings.l;

-- finding individual player stats from last 3 conference championship teams



from(
  select team.team,
         standings.season
  from team_info as team
  inner join team_standings as standings on standings.team = team.team
  where standings.gb = 0 and
        standings.season >= 2013
  order by season desc
    )





-- finding teams with the most players that shot above the average in 3Pt_pct

select a.team,
       a.season,
       team_standings.w as wins,
       team_standings.gb as games_back,
       count(a.name) as player_count
from (
  select player_info.name as name,
         player.season as season,
         player.team as team
  from (
        select *
        from RegularSeasonAverages
        where `3P%` > (select avg(`3P%`) from RegularSeasonAverages) and
              substring(season, 6, 2) >= 13 and
              substring(`3pm-a`, 5, 2) >= 2.5
        ) as player
  inner join player_info on player_info.player_id = player.player_id
      ) as a
inner join team_standings on team_standings.team = a.team and substring(team_standings.season, 3, 2) = substring(a.season, 6, 2)
where team_standings.w >= 41
group by a.team,
         a.season,
         team_standings.w,
         team_standings.gb
order by a.team asc,
         a.season desc
