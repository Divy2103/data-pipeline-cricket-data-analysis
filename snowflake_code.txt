select * from "cricbuzz_table";
-- truncate table "cricbuzz_table";

-- select * from player_batting_stats;

-- select * from player_bowling_stats;

-- create or replace stream icc_ranking_stream
-- on table "cricbuzz_table"
-- append_only = true;

select *, row_number() over (order by "isWomen","formatType","category","rank") from icc_ranking_stream;
select count(*) from icc_ranking_stream group by "isWomen","formatType","category","rank";

-- create or replace table icc_rankings_main(
--     id number(10,0),
--     player_rank number(10,0),
--     row_no number(10,0),
--     name varchar(50),
--     country varchar(20),
--     rating number(5),
--     difference number(10,0),
--     points number(5),
--     lastUpdatedOn timestamp_tz,
--     trend varchar(5),
--     faceImageId number(10,0),
--     countryId number(10),
--     isWomen varchar(10),
--     formatType varchar(10),
--     category varchar(20)
-- );

merge into icc_rankings_main as target 
using (
    select 
    "id" as id,
    "rank" as player_rank,
    row_number() over (order by "isWomen","formatType","category","rank") as row_no,
    "name" as name,
    "country" as country,
    "rating" as rating,
    "difference" as difference,
    "points" as points,
    "lastUpdatedOn" as lastUpdatedOn,
    "trend" as trend,
    "faceImageId" as faceImageId,
    "countryId" as countryId,
    "isWomen" as isWomen,
    "formatType" as formatType,
    "category" as category
    from icc_ranking_stream
) as source 
-- on target.player_rank = source.player_rank and target.isWomen = source.isWomen and target.formatType = source.formatType and target.category = source.category
on target.row_no = source.row_no
when matched then 
update set 
        target.id = source.id,
        target.player_rank = source.player_rank,
        target.name = source.name,
        target.country = source.country,
        target.rating = source.rating,
        target.difference = source.difference,
        target.points = source.points,
        target.lastUpdatedOn = source.lastUpdatedOn,
        target.trend = source.trend,
        target.isWomen = source.isWomen,
        target.formatType = source.formatType,
        target.faceImageId = source.faceImageId,
        target.countryId = source.countryId,
        target.category = source.category

when not matched then 
    insert (
        id,
        player_rank,
        row_no,
        name,
        country,
        rating,
        difference,
        lastUpdatedOn,
        trend,
        faceImageId,
        points,
        countryId,
        isWomen,
        formatType,
        category
    )
    values (
        source.id,
        source.player_rank,
        source.row_no,
        source.name,
        source.country,
        source.rating,
        source.difference,
        source.lastUpdatedOn,
        source.trend,
        source.faceImageId,
        source.points,
        source.countryId,
        source.isWomen,
        source.formatType,
        source.category
    );

-- select count(player_rank),player_rank from icc_rankings_main group by player_rank;
select * from icc_rankings_main;


select * from player_batting_stats;

select * from "team_list";
select * from "team_players";
-- truncate table "team_players";

create or replace stream team_players_stream
on table "team_players"
append_only = true;

select * from team_players_stream;

select distinct "id","name","battingStyle","bowlingStyle","role","teamId" from team_players_stream;
-- create table dummy like "team_players";
-- merge into dummy as target
-- using (
-- select distinct
-- )

-- create or replace table team_players_main (
--     id number(10,0),
--     name varchar(50),
--     battingStyle varchar(50),
--     bowlingStyle varchar(50),
--     role varchar(50),
--     teamId number(10,0)
-- );

merge into team_players_main as target 
using (
    select 
        "id" as id,
        "name" as name,
        "battingStyle" as battingStyle,
        "bowlingStyle" as bowlingStyle,
        "role" as role,
        "teamId" as teamId
    from team_players_stream
) as source 
on target.id = source.id and target.teamId = source.teamId
when matched then 
update set 
        target.id = source.id,
        target.name = source.name,
        target.battingStyle = source.battingStyle,
        target.bowlingStyle = source.bowlingStyle,
        target.role = source.role,
        target.teamId = source.teamId
when not matched then 
    insert (
        id,
        name,
        battingStyle,
        bowlingStyle,
        role,
        teamId
    )
    values (
        source.id,
        source.name,
        source.battingStyle,
        source.bowlingStyle,
        source.role,
        source.teamId
    );

select id,count(id) from team_players_main group by id order by count(id) desc;
-- delete from team_players_main where id in (8808);


select playerid from player_batting_stats;
select * from player_batting_stats_stream;
-- truncate table player_batting_stats;


-- create or replace stream player_batting_stats_stream
-- on table player_batting_stats
-- append_only = true;

select playerid,format from player_batting_stats_stream group by playerid,format;

-- create or replace table player_batting_stats_main(
--     PLAYERID NUMBER(10,0),
--     PLAYERNAME VARCHAR(20),
--     FORMAT VARCHAR(20),
--     MATCHES NUMBER(10,0),
--     INNINGS NUMBER(10,0),
--     RUNS NUMBER(10,0),
--     BALLS NUMBER(10,0),
--     HIGHEST NUMBER(10,0),
--     AVERAGE NUMBER(10,2),
--     SR NUMBER(10,2),
--     NOTOUT NUMBER(10,0),
--     FOURS NUMBER(10,0),
--     SIXES NUMBER(10,0),
--     DUCKS NUMBER(10,0),
--     FIFTIES NUMBER(10,0),
--     HUNDREDS NUMBER(10,0),
--     DOUBLEHUNDREDS NUMBER(10,0),
--     TRIPLEHUNDREDS NUMBER(10,0),
--     QUADRUPLEHUNDREDS NUMBER(10,0)
-- );


merge into player_batting_stats_main as target
using (
select 
    PLAYERID,
    PLAYERNAME,
    FORMAT,
    MATCHES,
    INNINGS,
    RUNS,
    BALLS,
    HIGHEST,
    AVERAGE,
    SR,
    NOTOUT,
    FOURS ,
    SIXES,
    DUCKS,
    FIFTIES,
    HUNDREDS,
    DOUBLEHUNDREDS,
    TRIPLEHUNDREDS,
    QUADRUPLEHUNDREDS
    from player_batting_stats_stream
) as source
on target.PLAYERID = source.PLAYERID and target.FORMAT = source.FORMAT
when matched then 
    update set 
        target.PLAYERNAME = source.PLAYERNAME,
        target.MATCHES = source.MATCHES,
        target.INNINGS = source.INNINGS,
        target.RUNS = source.RUNS,
        target.BALLS = source.BALLS,
        target.HIGHEST = source.HIGHEST,
        target.AVERAGE = source.AVERAGE,
        target.SR = source.SR,
        target.NOTOUT = source.NOTOUT,
        target.FOURS = source.FOURS,
        target.SIXES = source.SIXES,
        target.DUCKS = source.DUCKS,
        target.FIFTIES = source.FIFTIES,
        target.HUNDREDS = source.HUNDREDS,
        target.DOUBLEHUNDREDS = source.DOUBLEHUNDREDS,
        target.TRIPLEHUNDREDS = source.TRIPLEHUNDREDS,
        target.QUADRUPLEHUNDREDS = source.QUADRUPLEHUNDREDS
when not matched then
insert (
        PLAYERID,
        PLAYERNAME,
        FORMAT,
        MATCHES,
        INNINGS,
        RUNS,
        BALLS,
        HIGHEST,
        AVERAGE,
        SR,
        NOTOUT,
        FOURS ,
        SIXES,
        DUCKS,
        FIFTIES,
        HUNDREDS,
        DOUBLEHUNDREDS,
        TRIPLEHUNDREDS,
        QUADRUPLEHUNDREDS
    )
    values (
        source.PLAYERID,
        source.PLAYERNAME,
        source.FORMAT,
        source.MATCHES,
        source.INNINGS,
        source.RUNS,
        source.BALLS,
        source.HIGHEST,
        source.AVERAGE,
        source.SR,
        source.NOTOUT,
        source.FOURS ,
        source.SIXES,
        source.DUCKS,
        source.FIFTIES,
        source.HUNDREDS,
        source.DOUBLEHUNDREDS,
        source.TRIPLEHUNDREDS,
        source.QUADRUPLEHUNDREDS
    );


select * from player_batting_stats_main order by playerid;


select * from player_bowling_stats;

-- create or replace stream player_bowling_stats_stream
-- on table player_bowling_stats
-- append_only = true;

select * from player_bowling_stats_stream;

-- create or replace table PLAYER_BOWLING_STATS_MAIN(
--     PLAYERID NUMBER(10,0),
--     PLAYERNAME VARCHAR(20),
--     FORMAT VARCHAR(20),
--     MATCHES NUMBER(10,0),
--     INNINGS NUMBER(10,0),
--     BALLS NUMBER(10,0),
--     RUNS NUMBER(10,0),
--     MAIDENS NUMBER(10,0),
--     WICKETS NUMBER(10,0),
--     AVG NUMBER(10,2),
--     ECO NUMBER(10,2),
--     SR NUMBER(10,2),
--     BBI VARCHAR(20),
--     BBM VARCHAR(20),
--     FOURW NUMBER(10,0),
--     FIVEW NUMBER(10,0),
--     TENW NUMBER(10,0)
-- );


merge into PLAYER_BOWLING_STATS_MAIN as target
using (
    select 
        PLAYERID ,
        PLAYERNAME,
        FORMAT,
        MATCHES ,
        INNINGS ,
        BALLS ,
        RUNS ,
        MAIDENS ,
        WICKETS ,
        AVG ,
        ECO ,
        SR ,
        BBI,
        BBM,
        FOURW ,
        FIVEW ,
        TENW
    from player_bowling_stats_stream
) as source
on target.PLAYERID = source.PLAYERID and target.FORMAT = source.FORMAT
when matched then
    update set 
        target.PLAYERNAME = source.PLAYERNAME,
        target.MATCHES = source.MATCHES,
        target.INNINGS = source.INNINGS,
        target.BALLS = source.BALLS,
        target.RUNS = source.RUNS,
        target.MAIDENS = source.MAIDENS,
        target.WICKETS = source.WICKETS,
        target.AVG = source.AVG,
        target.ECO = source.ECO,
        target.SR = source.SR,
        target.BBI = source.BBI,
        target.BBM = source.BBM,
        target.FOURW = source.FOURW,
        target.FIVEW = source.FIVEW,
        target.TENW = source.TENW
when not matched then
    insert(
        PLAYERID ,
        PLAYERNAME,
        FORMAT,
        MATCHES ,
        INNINGS ,
        BALLS ,
        RUNS ,
        MAIDENS ,
        WICKETS ,
        AVG ,
        ECO ,
        SR ,
        BBI,
        BBM,
        FOURW ,
        FIVEW ,
        TENW 
    )
    values(
        source.PLAYERID ,
        source.PLAYERNAME ,
        source.FORMAT ,
        source.MATCHES ,
        source.INNINGS,
        source.BALLS ,
        source.RUNS ,
        source.MAIDENS,
        source.WICKETS ,
        source.AVG ,
        source.ECO ,
        source.SR ,
        source.BBI ,
        source.BBM ,
        source.FOURW ,
        source.FIVEW ,
        source.TENW
    );


select * from player_bowling_stats_main order by playerid;

select * from icc_rankings_main;
select * from player_batting_stats_main;
select * from player_bowling_stats_main;
select * from team_players_main;
select * from "team_list";
-- select * from "team_players"

-- truncate table battingperformance;

-- select * from wickets;

select * from series;
-- truncate table series;
-- create or replace stream series_stream
-- on table series
-- append_only = true;
select * from series_stream;

select * from matches;
-- create or replace stream matches_stream
-- on table matches
-- append_only = true;
select * from matches_stream;

select * from innings;
-- create or replace stream innings_stream
-- on table innings
-- append_only = true;
select * from innings_stream;

select * from battingperformance;
-- create or replace stream battingperformance_stream
-- on table battingperformance
-- append_only = true;
select * from battingperformance_stream;

select * from bowlingperformance;
-- create or replace stream bowlingperformance_stream
-- on table bowlingperformance
-- append_only = true;
select * from bowlingperformance_stream;

select * from partnerships;
-- create or replace stream partnerships_stream
-- on table partnerships
-- append_only = true;
select * from partnerships_stream;

select * from wickets;
-- create or replace stream wickets_stream
-- on table wickets
-- append_only = true;
select * from wickets_stream;


create table series_main(
    SERIESID NUMBER(10,0),
    SERIESNAME string,
    SERIESDESC string,
    YEAR int
);

CREATE TABLE Matches_main (
    matchId number(10,0) PRIMARY KEY,
    seriesId number(10,0),
    matchDescription STRING,
    matchFormat STRING,
    matchType STRING,
    matchStartTimestamp TIMESTAMP,
    matchCompleteTimestamp TIMESTAMP,
    tossWinnerId number(10,0),
    tossWinnerName STRING,
    decision STRING,
    winningTeam STRING,
    winningteamId number(10,0),
    winningMargin number(10,0),
    winByRuns BOOLEAN,
    winByInnings BOOLEAN,
    status STRING,
    FOREIGN KEY (seriesId) REFERENCES Series(seriesId)
);

CREATE TABLE Innings_main (
    inningsId number(10,0),
    matchId number(10,0),
    battingTeamId number(10,0),
    bowlingTeamId number(10,0),
    totalRuns number(10,0),
    totalWickets number(10,0),
    overs number(10,2),
    runRate number(10,2)
);

CREATE TABLE BattingPerformance_main (
    matchId number(10,0),
    inningsId number(10,0),
    batsmanId number(20,0),
    batsmanName varchar(100),
    runs number(10,0),
    balls number(10,0),
    fours number(10,0),
    sixes number(10,0),
    strikeRate number(10,2),
    outDesc STRING,
    bowlerId number(10,0),
    fielderId1 number(10,0),
    fielderId2 number(10,0),
    wicketCode STRING
);

CREATE TABLE BowlingPerformance_main (
    matchId number(10,0),
    inningsId number(10,0),
    bowlerId number(10,0),
    bowlerName varchar(100),
    overs number(10,2),
    maidens number(10,0),
    runsConceded number(10,0),
    wickets number(10,0),
    economy number(10,2),
    noBalls number(10,0),
    wides number(10,0),
    dots number(10,0)
);

CREATE TABLE Wickets_main (
    matchId number(10,0),
    inningsId number(10,0),
    wktNbr number(10,0),
    batId number(10,0),
    batName varchar(100),
    wktOver number(10,2),
    wktRuns number(10,0)
);

CREATE TABLE Partnerships_main (
    matchId number(10,0),
    inningsId number(10,0),
    partnershipNbr number(10,0),
    bat1Id number(10,0),
    bat1Name varchar(100),
    bat1Runs number(10,0),
    bat1balls number(10,0),
    bat2Id number(10,0),
    bat2Name varchar(100),
    bat2Runs number(10,0),
    bat2balls number(10,0),
    totalRuns number(10,0),
    totalBalls number(10,0)
);



merge into series_main as target 
using (
    select 
        SERIESID,
        SERIESNAME,
        SERIESDESC,
        YEAR
    from series_stream
) as source 
on target.SERIESID = source.SERIESID
when matched then 
update set 
        target.SERIESNAME = source.SERIESNAME,
        target.SERIESDESC = source.SERIESDESC,
        target.YEAR = source.YEAR
when not matched then 
    insert (
        SERIESID,
        SERIESNAME,
        SERIESDESC,
        YEAR
    )
    values (
        source.SERIESID,
        source.SERIESNAME,
        source.SERIESDESC,
        source.YEAR
    );


merge into matches_main as target 
using (
    select 
        MATCHID,
        SERIESID,
        MATCHDESCRIPTION,
        MATCHFORMAT,
        MATCHTYPE,
        MATCHSTARTTIMESTAMP,
        MATCHCOMPLETETIMESTAMP,
        TOSSWINNERID,
        TOSSWINNERNAME,
        DECISION,
        WINNINGTEAM,
        WINNINGTEAMID,
        WINNINGMARGIN,
        WINBYRUNS,
        WINBYINNINGS,
        STATUS
    from matches_stream
) as source 
on target.MATCHID = source.MATCHID
when matched then 
update set 
        target.SERIESID = source.SERIESID,
        target.MATCHDESCRIPTION = source.MATCHDESCRIPTION,
        target.MATCHFORMAT = source.MATCHFORMAT,
        target.MATCHTYPE = source.MATCHTYPE,
        target.MATCHSTARTTIMESTAMP = source.MATCHSTARTTIMESTAMP,
        target.MATCHCOMPLETETIMESTAMP = source.MATCHCOMPLETETIMESTAMP,
        target.TOSSWINNERID = source.TOSSWINNERID,
        target.TOSSWINNERNAME = source.TOSSWINNERNAME,
        target.DECISION = source.DECISION,
        target.WINNINGTEAM = source.WINNINGTEAM,
        target.WINNINGTEAMID = source.WINNINGTEAMID,
        target.WINNINGMARGIN = source.WINNINGMARGIN,
        target.WINBYRUNS = source.WINBYRUNS,
        target.WINBYINNINGS = source.WINBYINNINGS,
        target.STATUS = source.STATUS
when not matched then 
    insert (
        MATCHID,
        SERIESID,
        MATCHDESCRIPTION,
        MATCHFORMAT,
        MATCHTYPE,
        MATCHSTARTTIMESTAMP,
        MATCHCOMPLETETIMESTAMP,
        TOSSWINNERID,
        TOSSWINNERNAME,
        DECISION,
        WINNINGTEAM,
        WINNINGTEAMID,
        WINNINGMARGIN,
        WINBYRUNS,
        WINBYINNINGS,
        STATUS
    )
    values (
        source.MATCHID,
        source.SERIESID,
        source.MATCHDESCRIPTION,
        source.MATCHFORMAT,
        source.MATCHTYPE,
        source.MATCHSTARTTIMESTAMP,
        source.MATCHCOMPLETETIMESTAMP,
        source.TOSSWINNERID,
        source.TOSSWINNERNAME,
        source.DECISION,
        source.WINNINGTEAM,
        source.WINNINGTEAMID,
        source.WINNINGMARGIN,
        source.WINBYRUNS,
        source.WINBYINNINGS,
        source.STATUS
    );


merge into innings_main as target 
using (
    select 
        INNINGSID,
        MATCHID,
        BATTINGTEAMID,
        BOWLINGTEAMID,
        TOTALRUNS,
        TOTALWICKETS,
        OVERS,
        RUNRATE
    from innings_stream
) as source 
on target.MATCHID = source.MATCHID and target.INNINGSID = source.INNINGSID
when matched then 
update set 
        target.BATTINGTEAMID = source.BATTINGTEAMID,
        target.BOWLINGTEAMID = source.BOWLINGTEAMID,
        target.TOTALRUNS = source.TOTALRUNS,
        target.TOTALWICKETS = source.TOTALWICKETS,
        target.OVERS = source.OVERS,
        target.RUNRATE = source.RUNRATE
when not matched then 
    insert (
        INNINGSID,
        MATCHID,
        BATTINGTEAMID,
        BOWLINGTEAMID,
        TOTALRUNS,
        TOTALWICKETS,
        OVERS,
        RUNRATE
    )
    values (
        source.INNINGSID,
        source.MATCHID,
        source.BATTINGTEAMID,
        source.BOWLINGTEAMID,
        source.TOTALRUNS,
        source.TOTALWICKETS,
        source.OVERS,
        source.RUNRATE
    );


MERGE INTO battingperformance_main AS target
USING (
    SELECT 
        MATCHID,
        INNINGSID,
        BATSMANID,
        BATSMANNAME,
        RUNS,
        BALLS,
        FOURS,
        SIXES,
        STRIKERATE,
        OUTDESC,
        BOWLERID,
        FIELDERID1,
        FIELDERID2,
        WICKETCODE
    FROM battingperformance_stream
) AS source 
ON target.MATCHID = source.MATCHID 
   AND target.INNINGSID = source.INNINGSID 
   AND target.BATSMANID = source.BATSMANID
WHEN MATCHED THEN 
UPDATE SET 
        target.BATSMANNAME = source.BATSMANNAME,
        target.RUNS = source.RUNS,
        target.BALLS = source.BALLS,
        target.FOURS = source.FOURS,
        target.SIXES = source.SIXES,
        target.STRIKERATE = source.STRIKERATE,
        target.OUTDESC = source.OUTDESC,
        target.BOWLERID = source.BOWLERID,
        target.FIELDERID1 = source.FIELDERID1,
        target.FIELDERID2 = source.FIELDERID2,
        target.WICKETCODE = source.WICKETCODE
WHEN NOT MATCHED THEN 
INSERT (
        MATCHID,
        INNINGSID,
        BATSMANID,
        BATSMANNAME,
        RUNS,
        BALLS,
        FOURS,
        SIXES,
        STRIKERATE,
        OUTDESC,
        BOWLERID,
        FIELDERID1,
        FIELDERID2,
        WICKETCODE
    )
VALUES (
        source.MATCHID,
        source.INNINGSID,
        source.BATSMANID,
        source.BATSMANNAME,
        source.RUNS,
        source.BALLS,
        source.FOURS,
        source.SIXES,
        source.STRIKERATE,
        source.OUTDESC,
        source.BOWLERID,
        source.FIELDERID1,
        source.FIELDERID2,
        source.WICKETCODE
    );


merge into bowlingperformance_main as target 
using (
    select 
        MATCHID,
        INNINGSID,
        BOWLERID,
        BOWLERNAME,
        OVERS,
        MAIDENS,
        RUNSCONCEDED,
        WICKETS,
        ECONOMY,
        NOBALLS,
        WIDES,
        DOTS
    from bowlingperformance_stream
) as source 
on target.MATCHID = source.MATCHID and target.INNINGSID = source.INNINGSID and target.BOWLERID = source.BOWLERID
when matched then 
update set 
        target.BOWLERNAME = source.BOWLERNAME,
        target.OVERS = source.OVERS,
        target.MAIDENS = source.MAIDENS,
        target.RUNSCONCEDED = source.RUNSCONCEDED,
        target.WICKETS = source.WICKETS,
        target.ECONOMY = source.ECONOMY,
        target.NOBALLS = source.NOBALLS,
        target.WIDES = source.WIDES,
        target.DOTS = source.DOTS
when not matched then 
    insert (
        MATCHID,
        INNINGSID,
        BOWLERID,
        BOWLERNAME,
        OVERS,
        MAIDENS,
        RUNSCONCEDED,
        WICKETS,
        ECONOMY,
        NOBALLS,
        WIDES,
        DOTS
    )
    values (
        source.MATCHID,
        source.INNINGSID,
        source.BOWLERID,
        source.BOWLERNAME,
        source.OVERS,
        source.MAIDENS,
        source.RUNSCONCEDED,
        source.WICKETS,
        source.ECONOMY,
        source.NOBALLS,
        source.WIDES,
        source.DOTS
    );


merge into partnerships_main as target 
using (
    select 
        MATCHID,
        INNINGSID,
        PARTNERSHIPNBR,
        BAT1ID,
        BAT1NAME,
        BAT1RUNS,
        BAT1BALLS,
        BAT2ID,
        BAT2NAME,
        BAT2RUNS,
        BAT2BALLS,
        TOTALRUNS,
        TOTALBALLS
    from partnerships_stream
) as source 
on target.MATCHID = source.MATCHID and target.INNINGSID = source.INNINGSID and target.PARTNERSHIPNBR = source.PARTNERSHIPNBR
when matched then 
update set 
        target.BAT1ID = source.BAT1ID,
        target.BAT1NAME = source.BAT1NAME ,
        target.BAT1RUNS = source.BAT1RUNS ,
        target.BAT1BALLS = source.BAT1BALLS,
        target.BAT2ID = source.BAT2ID,
        target.BAT2NAME = source.BAT2NAME,
        target.BAT2RUNS = source.BAT2RUNS,
        target.BAT2BALLS = source.BAT2BALLS,
        target.TOTALRUNS = source.TOTALRUNS,
        target.TOTALBALLS = source.TOTALBALLS
when not matched then 
    insert (
        MATCHID,
        INNINGSID,
        PARTNERSHIPNBR,
        BAT1ID,
        BAT1NAME,
        BAT1RUNS,
        BAT1BALLS,
        BAT2ID,
        BAT2NAME,
        BAT2RUNS,
        BAT2BALLS,
        TOTALRUNS,
        TOTALBALLS
    )
    values (
        source.MATCHID,
        source.INNINGSID,
        source.PARTNERSHIPNBR,
        source.BAT1ID,
        source.BAT1NAME,
        source.BAT1RUNS,
        source.BAT1BALLS,
        source.BAT2ID,
        source.BAT2NAME,
        source.BAT2RUNS,
        source.BAT2BALLS,
        source.TOTALRUNS,
        source.TOTALBALLS
    );

  
MERGE INTO WICKETS_MAIN  AS target
USING (
    SELECT 
        MATCHID,
        INNINGSID,
        WKTNBR,
        BATID,
        BATNAME,
        WKTOVER,
        WKTRUNS
    FROM WICKETS_STREAM
) AS source 
ON target.MATCHID = source.MATCHID 
   AND target.INNINGSID = source.INNINGSID 
   AND target.WKTNBR = source.WKTNBR
WHEN MATCHED THEN 
UPDATE SET 
        target.BATID = source.BATID,
        target.BATNAME = source.BATNAME,
        target.WKTOVER = source.WKTOVER,
        target.WKTRUNS = source.WKTRUNS
WHEN NOT MATCHED THEN 
INSERT (
        MATCHID,
        INNINGSID,
        WKTNBR,
        BATID,
        BATNAME,
        WKTOVER,
        WKTRUNS
    )
VALUES (
        source.MATCHID,
        source.INNINGSID,
        source.WKTNBR,
        source.BATID,
        source.BATNAME,
        source.WKTOVER,
        source.WKTRUNS
    );



select * from battingperformance_main;
select * from bowlingperformance_main where bowlerid = 8808 order by matchid,inningsid;
select * from icc_rankings_main;
select * from innings_main;
select * from matches_main;
select * from partnerships_main;
select * from player_batting_stats_main;
select * from player_bowling_stats_main;
select * from series_main;
select * from "team_list";
select * from team_players_main;
select * from wickets_main;

-- delete from battingperformance_main where matchid = 100321;
-- delete from bowlingperformance_main where matchid = 100321;


select * from series_main;
select * from matches_main;
select * from innings_main;

select * from wickets_main order by matchid,inningsid, wktnbr;
select * from partnerships_main order by matchid,inningsid, partnershipnbr;
-- delete from wickets_main where matchid = 100321;
select * from team_players_main;
select * from player_batting_stats_main;