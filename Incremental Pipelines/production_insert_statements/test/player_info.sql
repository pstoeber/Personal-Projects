select player_id,
       name,
       team,
       position,
       source_link,
       created_at,
       f.rowNumber() as rowNumber
from player_info
