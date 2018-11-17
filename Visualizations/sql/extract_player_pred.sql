select *
from vis_player_points_comparision
where team like 'Washington%' and
      game_date = (select max(game_date) from vis_player_points_comparision where team like 'Washington%')
