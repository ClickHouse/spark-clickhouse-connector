Playground
===

## For Users

### Setup

1. Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/);
2. Start compose services `cd docker && docker compose up`;

### Play

1. Connect using `beeline`(Recommended for beginners)

`docker exec -it kyuubi /opt/kyuubi/bin/beeline -u 'jdbc:hive2://0.0.0.0:10009/'`;

2. Connect using `spark-sql`

`docker exec -it kyuubi /opt/spark/bin/spark-sql`

3. Connect using `spark-shell`

`docker exec -it kyuubi /opt/spark/bin/spark-shell`

4. Connect using DataGrip or DBeaver

Add a new Hive or Spark SQL or Kyuubi datasource with

- connection url `jdbc:hive2://0.0.0.0:10009/`
- username: `root`
- password: `<empty>`

### Access Service

- MinIO: http://localhost:9001
- Spark UI: http://localhost:4040 (available after Spark application launching by Kyuubi, port may be 4041, 4042... if you launch more than one Spark applications)

### Shutdown

1. Stop the compose services by pressing `CTRL+C`; 
2. Remove the stopped containers `docker compose rm`;

## For Developers

In addition to play with the pre-build images, developers may want to build the playground with SNAPSHOT version of Spark ClickHouse Connector.

### Build

1. Follow [README](../README.md#build) to build the project;
2. Build images `cd docker && ./build-image.sh`;

### Setup

1. Launch dev compose services `docker compose --env-file .env-dev up`;

## For Maintainers

1. Keep `PROJECT_VERSION` up-to-date in `.env`, it should be the latest stable version;
2. Keep `PROJECT_VERSION` up-to-date in `.env-dev`, it should be same as `../version.txt`;
3. Publish the images to Docker Hub after the releasing jars available on Maven Central;
