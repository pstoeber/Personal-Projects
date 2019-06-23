select team_id,
       season,
       OWN as own_pts,
       OPP as opp_pts,
       DIFF as diff,
       source_link,
       created_at
from points
