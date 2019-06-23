select team_id,
       season,
       OWN as own_3pt_pct,
       OPP as opp_3pt_pct,
       `FT%` as ft_pct,
       source_link,
       created_at
from 3pt_pct
