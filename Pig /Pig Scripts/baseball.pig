--load basebasll data

baseball = load 'baseball' as (name:chararray,team:chararray,position:bag{(p:chararray)},bat:map[]);

--put unknown for players having positiom as null

nonempty = foreach  baseball generate name,team,((position is null or IsEmpty(position)?{('unknown')}:position) as position;     

--extract position bag

pos = foreach nonempty generate name,team,flatten(position) as position;    

--remove records that having null to position

filterbaseball = filter pos by not position matches 'unknown';

-- group by position

baseballpos = group filterbaseball by position;

--dump baseball pos

dump baseballpos

--output data

--(bolwer,{(Jorge Posada,New York Yankees,bolwer)})
--(Catcher,{(Landon Powell,Oakland Athletics,Catcher),(Jorge Posada,New York Yankees,Catcher)})
--(Pitcher,{(David Price,Tampa Bay Rays,Pitcher),(Scott Proctor,Florida Marlins,Pitcher)})
--(Infielder,{(Nick Punto,Minnesota Twins,Infielder),(Martín Prado,Atlanta Braves,Infielder)})
--(Shortstop,{(Nick Punto,Minnesota Twins,Shortstop)})
--(Outfielder,{(Jason Pridie,Minnesota Twins,Outfielder)})
--(Left_fielder,{(Martín Prado,Atlanta Braves,Left_fielder),(Jason Pridie,Minnesota Twins,Left_fielder),(Albert Pujols,St. Louis Cardinals,Left_fielder)})
--(First_baseman,{(Albert Pujols,St. Louis Cardinals,First_baseman),(Landon Powell,Oakland Athletics,First_baseman)})
--(Third_baseman,{(Nick Punto,Minnesota Twins,Third_baseman),(Albert Pujols,St. Louis Cardinals,Third_baseman)})
--(Center_fielder,{(Jason Pridie,Minnesota Twins,Center_fielder)})
--(Relief_pitcher,{(Scott Proctor,Florida Marlins,Relief_pitcher)})
--(Second_baseman,{(Nick Punto,Minnesota Twins,Second_baseman),(Martín Prado,Atlanta Braves,Second_baseman)})
--(Starting_pitcher,{(David Price,Tampa Bay Rays,Starting_pitcher)})
--(Designated_hitter,{(Jorge Posada,New York Yankees,Designated_hitter)})




