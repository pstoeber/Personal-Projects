create table if not exists box_score_map
(
	game_hash varchar(200) null,
	away_team varchar(50) null,
	home_team varchar(50) null,
	game_date date null
)
;
