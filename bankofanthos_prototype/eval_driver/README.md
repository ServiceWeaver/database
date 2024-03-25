## Prereq
- Install [Service Weaver](https://serviceweaver.dev/docs.html#what-is-service-weaver) locally
- Install [Docker](https://www.docker.com/get-started) locally

## Instruction
Before start the program, run startup script to initialize database for bank of anthos.
```shell
cd eval_driver
./startup.sh
```

Then running the program
```shell
go run .
```

## Connect to DB for develop
Note: DB port may be changed.
```shell
psql -h localhost -p 5432 -U postgres  # password: password
psql -h localhost -p 5432 -U admin postgresdbsnapshot # password: admin
psql -h localhost -p 5432 -U admin accountsdbsnapshot
```

## Running BOA app on service weaver 
```shell
# running baseline service
SERVICEWEAVER_CONFIG=weaver.toml go run .

# running experimental service
SERVICEWEAVER_CONFIG=weaver_experimental.toml go run .
```