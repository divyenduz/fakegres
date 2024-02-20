# Fakegres

Distributed PostgreSQL backed by SQLite.

Based on https://notes.eatonphil.com/distributed-postgres.html

In this repository,

1. bolt is replaced with SQLite for storing node data.
2. delete from table works (deletes everything without respecting the where clause)

```bash
$ go build
$ ./fakegres --node-id node1 --raft-port 2222 --http-port 8222 --pg-port 6000
$ ./fakegres --node-id node2 --raft-port 2223 --http-port 8223 --pg-port 6001
$ curl 'localhost:8222/add-follower?addr=localhost:2223&id=node2'
$ psql -h localhost -p 6000

psql> create table x (age int, name text);
psql> insert into x values(14, 'garry'), (20, 'ted');
psql> select name, age from x;
```
