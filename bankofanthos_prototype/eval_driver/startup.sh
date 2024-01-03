set -euo pipefail

main() {
# start neon
cd ~/neon
cargo neon stop
cargo neon init --force
cargo neon start
cargo neon tenant create --set-default
cargo neon endpoint create main
cargo neon endpoint start main

# establish database
psql -p55432 -U cloud_admin postgres -h 127.0.0.1 -c "CREATE USER admin WITH PASSWORD 'admin';"
psql -p55432 -U cloud_admin postgres -h 127.0.0.1 -c "CREATE DATABASE postgresdb WITH OWNER admin;"
psql -p55432 -U cloud_admin postgres -h 127.0.0.1 -c "CREATE DATABASE accountsdb WITH OWNER admin;"

psql -p55432 -h 127.0.0.1 postgresdb admin -f ~/prototype/bankofanthos_prototype/bankofanthos/postgresdb.sql
psql -p55432 -h 127.0.0.1 accountsdb admin -f ~/prototype/bankofanthos_prototype/bankofanthos/accountsdb.sql

POSTGRES_DB=postgresdb POSTGRES_USER=admin POSTGRES_PASSWORD=admin LOCAL_ROUTING_NUM=883745000 USE_DEMO_DATA=True ~/prototype/bankofanthos_prototype/bankofanthos/1_create_transactions.sh

openssl genrsa -out jwtRS256.key 4096
openssl rsa -in jwtRS256.key -outform PEM -pubout -out jwtRS256.key.pub
mkdir -p /tmp/.ssh
mv jwtRS256.key jwtRS256.key.pub /tmp/.ssh
}

main "$@"