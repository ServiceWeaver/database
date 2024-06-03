set -euo pipefail

main() {
docker run \
    --rm \
    --detach \
    --name postgres \
    --env POSTGRES_PASSWORD=password \
    --volume="$(realpath postgres.sh):/app/postgres.sh" \
    --volume="$(realpath postgresdb.sql):/app/postgresdb.sql" \
    --volume="$(realpath accountsdb.sql):/app/accountsdb.sql" \
    --volume="$(realpath 1_create_transactions.sh):/app/1_create_transactions.sh" \
    --volume="$(realpath postgresql.conf):/etc/postgres/postgresql.conf" \
    --publish 127.0.0.1:5432:5432 \
    postgres:15 \
    -c config_file=/etc/postgres/postgresql.conf

sleep 2

# establish database
docker exec -it postgres /app/postgres.sh

openssl genrsa -out jwtRS256.key 4096
openssl rsa -in jwtRS256.key -outform PEM -pubout -out jwtRS256.key.pub
mkdir -p /tmp/.ssh
mv jwtRS256.key jwtRS256.key.pub /tmp/.ssh
}

main "$@"
