select team_id,
       season,
       OFF as off_reb,
       DEF as def_reb,
       TOT as tot_reb,
       source_link,
       created_at
from rebound_pct
