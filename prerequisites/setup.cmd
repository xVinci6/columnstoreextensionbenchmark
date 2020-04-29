docker build -t postgres_12_cstore postgres_cstore_docker
docker run -d -p 5432:5432 --name postgres12 -e POSTGRES_PASSWORD=password postgres:12.2
docker run -d -p 5433:5432 --name postgres12cstore -e POSTGRES_PASSWORD=password postgres_12_cstore
