# Eval Driver Prototype on Bank Of Anthos
Eval driver runs diff testing for bank of anthos, it takes two versions of Bank Of Anthos and sampled requests as input, display response diff and database diff as output.

For the same set of requests, we run 4 different trials:
1. Send all requests to v1 as Control
2. Send all requests to v2 as Experimental_1
3. Send half request to v1, half request to v2 as Experimental_2
4. Send half request to v2, half request to v1  as Experimental_3

While we do diffs at the end, we compare Control and Experimental_*.

## Prereq
- Install [Service Weaver](https://serviceweaver.dev/docs.html#what-is-service-weaver) locally
- Install [Docker](https://www.docker.com/get-started) locally

## Instruction
### Step 1: Generate requests
Random generate requests for eval to run if there is no request log yet
```shell
cd tester
go run . [options]

# Options
-counts <requestCountPerUser>   Random generate deposit/withdraw requests counts per user, splited by ',' . Example: 3,4,5
```

### Step 2: Set up env for bank of anthos app
Replace with real file path in startup script. Then run startup script to initialize database for bank of anthos.
```shell
cd eval_driver
./startup.sh
```
Compile bank of anthos and get the binary
```shell
cd bankofanthos
go build .
```

### Step 3: Run eval to catch bugs in bank of anthos app
Finally run the eval
```shell
cd eval_driver
go run . [options]

# Options
-configFile <configFilePath>    Path for config file to define two versions and sampled requests
-deleteBranches <boolean>       Whether delete all branches after 
-inlineDiff <boolean>           Whether display database inline diff or side by side diff
-respDiff <boolean>             Whether display response diff or not
```

## Designed Bugs
Two bugs in the prototype will be caught during interleaving. Two canry versions are defined in different bank of anthos config files.
### BUG1
- V1 random generate accountd id with 10 digit
- V2 random generate accountd id with 12 digit
- forward compatibility is not handled properly
```shell
go run . -configFile=config.toml
```

### BUG2
- V1 use golang.org/x/crypto/bcrypt to hash password
- V2 use base64.StdEncoding to hash password
- Both backward and forward compatibility are not handled properly
```shell
go run . -configFile=config_v2.toml
```

### BUG3
- V1 successfully load all rows from currency table to memory when service starts
- V2 load partial currency table to memory since there is newly inserted rows exceed currency limitation, causing\
common currency missing
- Service restarts with newly inserted rows will cause bugs
```shell
go run . -configFile=config_v3.toml
```

### BUG4
- V1 displays all ssn digits, which is `111-11-1111`
- V2 has the wrong ssn format as `*****1-1111`, whish should be `***-**-1111`
- We can see the ssn column difference for database diffs.
```shell
go run . -configFile=config_v4.toml
```

## Developer Guide
### Connect to DB for developer
Login to docker terminal docker terminal
```shell
docker exec -it postgres /bin/bash
```

Connect to postgres database and check tables and schemas
```shell
psql -h localhost -p 5432 -U admin postgresdb # password: admin
psql -h localhost -p 5432 -U admin accountsdb
```

### Running BOA app on service weaver 
```shell
# running baseline service
SERVICEWEAVER_CONFIG=weaver.toml go run .

# running canary service
SERVICEWEAVER_CONFIG=weaver_canary.toml go run .
```
