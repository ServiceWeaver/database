## Prereq
- Install [neon](https://github.com/neondatabase/neon) locally under home directory

- Install [Service Weaver](https://serviceweaver.dev/docs.html#what-is-service-weaver) locally

## Instruction
Before start the program, run startup script to establish the neon database
```shell
cd eval_driver
./startup.sh
```

Then running the program
```shell
go run .
```

## Connect to DB
Note: DB port may be changed.
```shell
# for clone baseline postgresdb
psql -p55436 -h 127.0.0.1 -U admin postgresdb

# for clone baseline accountsdb
psql -p55436 -h 127.0.0.1 -U admin accountsdb

# for clone experimental postgresdb
psql -p55438 -h 127.0.0.1 -U admin postgresdb

# for clone experimental accountsdb
psql -p55438 -h 127.0.0.1 -U admin accountsdb
```

## Check Cloned DB banch
```shell
cd ~/neon
cargo neon timeline list
```

## Running BOA app on service weaver 
```shell
# running baseline service
SERVICEWEAVER_CONFIG=weaver.toml go run .

# running experimental service
SERVICEWEAVER_CONFIG=weaver_experimental.toml go run .
```