docker-compose -f ../docker-compose.yml down
docker volume prune --force
docker-compose -f ../docker-compose.yml build "$@" --parallel
docker-compose -f ../docker-compose.yml up
