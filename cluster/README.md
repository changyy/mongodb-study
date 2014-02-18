Init without auth
=================
$ python replset_init.py --reset
~~~
Init DB(0), Port: 30000, Path: /home/id/data/mongodb-study/cluster/db/db-0
 $ mongod --dbpath /home/id/data/mongodb-study/cluster/db/db-0 --port 30000 --oplogSize 700 --logpath /home/id/data/mongodb-study/cluster/log/db-0.log --rest --replSet firstset

Init DB(1), Port: 30001, Path: /home/id/data/mongodb-study/cluster/db/db-1
 $ mongod --dbpath /home/id/data/mongodb-study/cluster/db/db-1 --port 30001 --oplogSize 700 --logpath /home/id/data/mongodb-study/cluster/log/db-1.log --rest --replSet firstset

Init DB(2), Port: 30002, Path: /home/id/data/mongodb-study/cluster/db/db-2
 $ mongod --dbpath /home/id/data/mongodb-study/cluster/db/db-2 --port 30002 --oplogSize 700 --logpath /home/id/data/mongodb-study/cluster/log/db-2.log --rest --replSet firstset

nohup: ignoring input and appending output to `nohup.out'
nohup: ignoring input and appending output to `nohup.out'
nohup: ignoring input and appending output to `nohup.out'

Waiting...
        localhost:30000: .....OK
        localhost:30001: OK
        localhost:30002: OK

Connect to localhost:30000

Initialize the First Replica Set: 

$ monogo localhost:30000/admin
mongo> db.runCommand( {'replSetInitiate': {'_id': 'firstset', 'members': [{'host': 'localhost:30000', '_id': 1}, {'host': 'localhost:30001', '_id': 2}, {'host': 'localhost:30002', '_id': 3}]}} )

Result:
{u'info': u'Config now saved locally.  Should come online in about a minute.', u'ok': 1.0}

All is done.

server info:
 $ mongo localhost:30000/admin --eval 'printjson(rs.status())'

shutdown servers:
 $ mongo localhost:30000/admin --eval 'db.shutdownServer()'
 $ mongo localhost:30001/admin --eval 'db.shutdownServer()'
 $ mongo localhost:30002/admin --eval 'db.shutdownServer()'
~~~

Init with auth & keyfile
========================

$ python replset_init.py --reset --auth-key-file keyfile --auth-user account --auth-pass password

~~~
Init DB(0), Port: 30000, Path: /home/id/data/mongodb-study/cluster/db/db-0
 $ mongod --dbpath /home/id/data/mongodb-study/cluster/db/db-0 --port 30000 --oplogSize 700 --logpath /home/id/data/mongodb-study/cluster/log/db-0.log --rest
nohup: ignoring input and appending output to `nohup.out'

Waiting...
        localhost:30000: ..OK

Connect to localhost:30000

Initialize the First Replica Set: 

$ monogo localhost:30000/admin
mongo> db.addUser( {user: "account", pwd:"password", roles:["userAdminAnyDatabase"] })

Add account done.

Restart the mongod:

Init DB(0), Port: 30000, Path: /home/id/data/mongodb-study/cluster/db/db-0
 $ mongod --dbpath /home/id/data/mongodb-study/cluster/db/db-0 --port 30000 --oplogSize 700 --logpath /home/id/data/mongodb-study/cluster/log/db-0.log --rest --replSet firstset --keyFile keyfile

Init DB(1), Port: 30001, Path: /home/id/data/mongodb-study/cluster/db/db-1
 $ mongod --dbpath /home/id/data/mongodb-study/cluster/db/db-1 --port 30001 --oplogSize 700 --logpath /home/id/data/mongodb-
study/cluster/log/db-1.log --rest --replSet firstset --keyFile keyfile

nohup: ignoring input and appending output to `nohup.out'
Init DB(2), Port: 30002, Path: /home/id/data/mongodb-study/cluster/db/db-2
 $ mongod --dbpath /home/id/data/mongodb-study/cluster/db/db-2 --port 30002 --oplogSize 700 --logpath /home/id/data/mongodb-
study/cluster/log/db-2.log --rest --replSet firstset --keyFile keyfile

nohup: ignoring input and appending output to `nohup.out'
nohup: ignoring input and appending output to `nohup.out'
nohup: ignoring input and appending output to `nohup.out'

Waiting...
        localhost:30000: .......OK
        localhost:30001: ...................................OK
        localhost:30002: .OK

Connect to localhost:30000

Initialize the First Replica Set: 

$ monogo localhost:30000/admin
mongo> db.runCommand( {'replSetInitiate': {'_id': 'firstset', 'members': [{'host': 'localhost:30000', '_id': 1}, {'host': 'localhost:30001', '_id': 2}, {'host': 'localhost:30002', '_id': 3}]}} )

Result:
{u'info': u'Config now saved locally.  Should come online in about a minute.', u'ok': 1.0}

All is done.

server info:
 $ mongo localhost:30000/admin -u account -p password --eval 'printjson(rs.status())'

shutdown servers:
 $ mongo localhost:30000/admin -u account -p password --eval 'db.shutdownServer()'
 
 $ mongo localhost:30001/admin -u account -p password --eval 'db.shutdownServer()'
 $ mongo localhost:30002/admin -u account -p password --eval 'db.shutdownServer()'
~~~

Usage
=====

$ monogo localhost:30000/admin
$ monogo localhost:30000/admin -u account -p password
