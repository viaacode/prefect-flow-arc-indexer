docker network create elastic
docker pull docker.elastic.co/elasticsearch/elasticsearch:8.14.1
docker stop /es01
docker rm /es01
docker run --name es01 --net elastic -p 9200:9200 -m 3GB -d docker.elastic.co/elasticsearch/elasticsearch:8.14.1


docker stop /test-postgres
docker rm /test-postgres
docker run --name test-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres 

sleep 30;
export ELASTIC_PASSWORD="elk-password"
docker exec -u postgres /test-postgres  bash -c "psql -c \"CREATE TABLE index (name varchar(255), function varchar(255), updated_at timestamptz DEFAULT now() NULL);\""
docker exec -u postgres /test-postgres  bash -c "psql -c \"INSERT INTO index (name, function) VALUES ('Lennert', 'Data engineer'), ('Milan', 'Data engineer'), ('Miel', 'Data architect'), (Bart, 'Solutions architect');\""
# docker exec es01 bash -c 'printf "elk-password\nelk-password" | ./bin/elasticsearch-reset-password -b -i -u elastic'
docker exec es01 bash -c 'printf "elk-password\nelk-password" | ./bin/elasticsearch-reset-password -b -i -u elastic'
docker cp es01:/usr/share/elasticsearch/config/certs/http_ca.crt .
sleep 5;
curl --cacert http_ca.crt -u elastic:$ELASTIC_PASSWORD -X GET "https://localhost:9200/_cat/indices/arc*?v=true&s=index&pretty"
python flows/main_flow.py
sleep 5;
curl --cacert http_ca.crt -u elastic:$ELASTIC_PASSWORD -X GET "https://localhost:9200/_cat/indices/arc*?v=true&s=index&pretty"
curl --cacert http_ca.crt -u elastic:$ELASTIC_PASSWORD -X GET "https://localhost:9200/arcv3/_search?pretty=true&q=*:*"
