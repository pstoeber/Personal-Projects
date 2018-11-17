/*select *
from vis_total_points_comparision
where team like 'Washington%'
*/
select *
from vis_total_points_comparision
where game_date = (select max(game_date) from vis_total_points_comparision)
