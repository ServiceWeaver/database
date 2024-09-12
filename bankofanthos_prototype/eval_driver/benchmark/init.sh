# Init database script
# psql -U postgres -p 5433 -h localhost

#!/bin/bash
count="$1"  

echo "count": $count

export POSTGRES_DB=benchmark_15gb POSTGRES_USER=postgres POSTGRES_PASSWORD=postgres PORT=5433 MIN_LEN=3 MAX_LEN=10

generate_username() {
    length=$(( (RANDOM % ($MAX_LEN - $MIN_LEN + 1)) + $MIN_LEN ))
    string= tr -dc A-Za-z0-9 </dev/urandom | head -c $length
    echo "$string"
}

generate_password() {
    length=$(( (RANDOM % ($MAX_LEN - $MIN_LEN + 1)) + $MIN_LEN ))
    string= tr -dc A-Za-z0-9 </dev/urandom | head -c $length
    echo "$string"
}

insert_rows(){
    for i in $(seq 1 $count);  do 
    insert_row
    done
}

insert_row() {
  name=$(generate_username)
  password=$(generate_password)
  PGPASSWORD="$POSTGRES_PASSWORD" psql -p $PORT -h 127.0.0.1 -X  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  INSERT INTO USERS_PK(username, password) VALUES ('$name','$password');
  INSERT INTO USERS(username, password) VALUES ('$name','$password');
EOSQL
}

main() {
  psql -U postgres -p 5433 -h localhost $POSTGRES_DB -f init.sql

  insert_rows
}

main
