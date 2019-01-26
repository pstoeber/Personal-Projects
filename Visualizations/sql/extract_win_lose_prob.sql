select lose_probability,
       win_probability
from win_predictions_comparison_view
where team like 'Los Angeles Lakers' and
      game_date = (select max(game_date) from win_predictions_comparison_view where team like 'Los Angeles Lakers')
