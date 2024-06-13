# Init database script
# psql -U postgres -p 5433 -h localhost

#!/bin/bash
count="$1"  

echo "count": $count

export POSTGRES_DB=benchmark POSTGRES_USER=postgres POSTGRES_PASSWORD=postgres PORT=5433 MIN_LEN=3 MAX_LEN=10

generate_username() {
    chars='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

    length=$(( (RANDOM % ($MAX_LEN - $MIN_LEN + 1)) + $MIN_LEN ))
    string=''

    for (( i=0; i<length; i++ )); do
    random_index=$((RANDOM % ${#chars}))  
    string+=${chars:random_index:1}
    done

    echo "$string"
}

generate_password() {
    chars='0123456789'

    length=$(( (RANDOM % ($MAX_LEN - $MIN_LEN + 1)) + $MIN_LEN ))
    string=''

    for (( i=0; i<length; i++ )); do
    random_index=$((RANDOM % ${#chars}))  
    string+=${chars:random_index:1}
    done

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
  psql -U postgres -p 5433 -h localhost benchmark -f init.sql

  insert_rows
}

main
