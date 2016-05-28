# scopely-redis
Scopely Redis Server for Scopely Challenge

To start execute the command "sbt run" from the root directory. 

Acceptable commands are:

SET key value

SET key value EX seconds (need not implement other SET options)

GET key

DEL key

DBSIZE

INCR key

ZADD key score member

ZCARD key

ZRANK key member

ZRANGE key start stop
