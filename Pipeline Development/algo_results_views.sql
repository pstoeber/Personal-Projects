/*

Predicted points vs actual points scored view

 */


select pred.player_id,
       pred.name,
       pred.team,
       pred.pts as predicted_points,
       pred.game_date,
       case
           when basic.pts is not null then basic.pts
           else 0
       end as actual_points
from nba_stats_backup.player_prediction_results as pred
inner join nba_stats_test.box_scores_map_view as map on ( (pred.team = map.team) and (str_to_date(pred.game_date, '%Y-%m-%d') = map.game_date) )
left outer join nba_stats_test.basic_box_stats as basic on ( (map.game_hash = basic.game_hash) and (pred.player_id = basic.player_id) )


/*

Team probability of win vs. actaul win/lose view

 */

select pred.team,
       pred.game_date,
       pred.win_probability,
       pred.lose_probability,

       case
           when map.home_away = 'Home' then results.home_score
           else results.away_score
       end as team_score,

       case
           when map.home_away = 'Away' then results.home_score
           else results.away_score
       end as opp_score

from nba_stats_backup.win_probability_results as pred
inner join nba_stats_test.box_scores_map_view as map on ( (pred.team = map.team) and (str_to_date(pred.game_date, '%Y-%m-%d') = map.game_date) )
inner join nba_stats_test.game_results as results on map.game_hash = results.game_hash
