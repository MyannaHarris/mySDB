# mySDB
mySDB => my Slow DataBase - because it is in Python

M-Harris

mySDB : mySlowDataBase

5-16-2018

## About mySDB:
mySDB is a Telnet server based key-pair database. It is written in Python and only holds data in memory. The database uses multithreading and locks to allow many users (clients) to connect to one server at the same time. The servers also can talk to each ther so if many are started and connected, they act like a distributed database.

## Running on localhost:
### Server
```
python mySDB.py 
or
python mySDB.py <PORT_NUMBER>
Ex: python mySDB.py 6377
```

### Client
```
telnet <HOST> <PORT_NUMBER>
Ex: telnet 127.0.0.1 6379
```

### Commands
```
SET <key> <value>
Ex: SET test "This is a test."

GET <key>
Ex; GET test

DEL <key1> <key2> ...
Ex: DEL test
Ex: DEL test1 test2

CLUSTER MEET <HOST> <PORT_NUMBER>
Ex: CLUSTER MEET 127.0.0.1 6377
```
