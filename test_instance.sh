docker network create elastic
docker pull docker.elastic.co/elasticsearch/elasticsearch:8.14.1
docker stop /es01
docker rm /es01
docker run --name es01 --net elastic -p 9200:9200 -m 3GB -d docker.elastic.co/elasticsearch/elasticsearch:8.14.1
docker stop /kib01


docker stop /test-postgres
docker rm /test-postgres
docker run --name test-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres 

sleep 30;
export ELASTIC_PASSWORD="elk-password"
docker rm /kib01
docker run --name kib01 --net elastic -p 5601:5601 -d docker.elastic.co/kibana/kibana:8.14.1
docker exec -u postgres /test-postgres  bash -c "psql -c \"CREATE TABLE index (name varchar(255), function varchar(255), updated_at timestamptz DEFAULT now() NULL);\""
docker exec -u postgres /test-postgres  bash -c "psql -c \"INSERT INTO index (name, function) VALUES ('Lennert', 'Data engineer'), ('Milan', 'Data engineer'), ('Miel', 'Data architect');\""
# docker exec es01 bash -c 'printf "elk-password\nelk-password" | ./bin/elasticsearch-reset-password -b -i -u elastic'
docker exec es01 /usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s kibana
docker exec es01 bash -c 'printf "elk-password\nelk-password" | ./bin/elasticsearch-reset-password -b -i -u elastic'

python flows/main_flow.py