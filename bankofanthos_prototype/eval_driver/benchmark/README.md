# Benchmark For R+/R- Database performance
## Prerequsite
- Install [Dolt](https://docs.dolthub.com/introduction/installation)
- Install [pg2mysql tool](https://docs.dolthub.com/introduction/installation) to convert postgres database to mysql database for Dolt setup
- Install [Postgres](https://www.postgresql.org/download/)

## Setup
Run script to create `benchmark` database with two tables users(username, password) and users_pk(<ins>username</ins>, password)
```code
./init.sh $row_num$
``` 

You can create multiple databases with different name by using ths script.

## Instruction
```code
go run . --dbUrlLists dbUrl1 --dbUrlLists dbUrl2 --dbUrlLIsts dbUrl3 ...
``` 
Pass multiple database urls to the command for benchmark.

All the stats will be dumped into a json file. Run the plot.py file to plot the benchmark stats.

```code
python plot.py
``` 

## Benchmarks
We benchmark clone one entire database, read/write performance and diffing two tables.
### Cloning
Create a table with size X
#### Baseline 1: Postgres
Dump the whole database and then restore the database
#### Baseline 2: Dolt
Create a new branch for the database
#### R+/R-
Create a new branch for the database

### Read/Write Performance
Write Y row into Table X With and Without Primary Key\
Read queries:
- 	SELECT * FROM TABLE WHERE username = 'aaaa';
- 	SELECT * FROM TABLE WHERE LENGTH(password) = 8 AND username LIKE 'a%';
-	SELECT COUNT(*) FROM TABLE;
-   SELECT * FROM TABLE
#### Baseline 1: Postgres
Execute Read/Write oprations on table
#### Baseline 2: Dolt
Execute Read/Write oprations on new branch
#### R+/R-
Execute Read/Write oprations on new branch

### Diffing
Diff table size X with Y modified rows
#### Baseline 1: Postgres
Compare two table with `SELECT * FROM TABLE A EXCEPT ALL SELECT * FROM TABLE B;` and `SELECT * FROM TABLE B EXCEPT ALL SELECT * FROM TABLE A;`

#### Baseline 2: Dolt
Diff two branches

#### R+/R-
Diff two branches
