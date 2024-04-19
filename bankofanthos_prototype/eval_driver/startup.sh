# set -euo pipefail

# main() {
    
# docker stop $(docker ps -q -f "name=postgres") 

# docker run \
#     --rm \
#     --detach \
#     --name postgres \
#     --env POSTGRES_PASSWORD=password \
#     --volume="$(realpath postgres.sh):/app/postgres.sh" \
#     --volume="$(realpath postgresdb.sql):/app/postgresdb.sql" \
#     --volume="$(realpath accountsdb.sql):/app/accountsdb.sql" \
#     --volume="$(realpath 1_create_transactions.sh):/app/1_create_transactions.sh" \
#     --publish 127.0.0.1:5432:5432 \
#     postgres:15

# sleep 2

# # establish database
# docker exec -it postgres /app/postgres.sh

# openssl genrsa -out jwtRS256.key 4096
# openssl rsa -in jwtRS256.key -outform PEM -pubout -out jwtRS256.key.pub
# mkdir -p /tmp/.ssh
# mv jwtRS256.key jwtRS256.key.pub /tmp/.ssh
# }

# main "$@"


set -euo pipefail

main() {
    
docker stop postgres 

# Run the Postgres instance.
docker run \
    --rm \
    --detach \
    --name postgres \
    --env POSTGRES_PASSWORD=password\
    --volume="/usr/local/google/home/zhukexin/database/bankofanthos_prototype/bankofanthos/postgres.sh:/app/postgres.sh" \
    --volume="/usr/local/google/home/zhukexin/database/bankofanthos_prototype/bankofanthos/postgresdb.sql:/app/postgresdb.sql" \
    --volume="/usr/local/google/home/zhukexin/database/bankofanthos_prototype/bankofanthos/accountsdb.sql:/app/accountsdb.sql" \
    --volume="/usr/local/google/home/zhukexin/database/bankofanthos_prototype/bankofanthos/1_create_transactions.sh:/app/1_create_transactions.sh" \
    --volume="/usr/local/google/home/zhukexin/database/bankofanthos_prototype/eval_driver/postgresql.conf:/etc/postgres/postgresql.conf" \
    --publish 127.0.0.1:5432:5432 \
    postgres:15 \
    -c config_file=/etc/postgres/postgresql.conf
sleep 5

# establish database
docker exec -it postgres /app/postgres.sh

openssl genrsa -out jwtRS256.key 4096
openssl rsa -in jwtRS256.key -outform PEM -pubout -out jwtRS256.key.pub
mkdir -p /tmp/.ssh
mv jwtRS256.key jwtRS256.key.pub /tmp/.ssh
}
 main "$@"