create table if not exists basic_box_stats
(
	name varchar(100) null,
	game_hash varchar(200) null,
	MP varchar(20) null,
	FG varchar(20) null,
	FGA varchar(20) null,
	FG_PCT varchar(20) null,
	`3P` varchar(20) null,
	`3PA` varchar(20) null,
	`3P_PCT` varchar(20) null,
	FT varchar(20) null,
	FTA varchar(20) null,
	FT_PCT varchar(20) null,
	ORB varchar(20) null,
	DRB varchar(20) null,
	TRB varchar(20) null,
	AST varchar(20) null,
	STL varchar(20) null,
	BLK varchar(20) null,
	TOV varchar(20) null,
	PF varchar(20) null,
	PTS varchar(20) null,
	PLUS_MINUS varchar(20) null
)
;
