#!/usr/bin/env bash

echo $(pwd)

docker-compose -f ./integration_tests/docker-compose.yaml up -d

function ping_cassandra() {
  docker exec -it cassandra /opt/bitnami/cassandra/bin/nodetool status | grep UN
  res=$?
}

echo "Waiting for Cassandra to be ready."

# Wait for Cassandra to be ready
max_attempts=40
cur_attempts=0
ping_cassandra
while (( res != 0 && ++cur_attempts != max_attempts ))
do
  ping_cassandra
  echo Attempt: $cur_attempts of $max_attempts
  sleep 1
done

if (( cur_attempts == max_attempts )); then
  echo "Cassandra Timed Out."
  exit 1
fi

echo "Waiting additional time for Cassandra to be ready."
# The Cassandra image takes more time to be ready despite
# nodetool-status being success.
# There has to be a better way than this.
sleep 40

go run ./examples/example.go
