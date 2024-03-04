# Databricks Apache Spark Associtate Developer Certification Study Guide
Repository with resources used for preparation to Databricks Certified Associate Developer - Apache Spark (Pyspark)

## Creating a local Pyspark cluster with Docker Compose
A good way to understand how Apache Spark works, is by deploying it locally on your machine through Docker. You can even leverage Docker Compose to create virtual workers that could replicate a Spark cluster. In thi session we are going to describe how you could do it.

### Installing Docker and Docker Container
The first step is to install Docker and Docker Compose.
You can find tutorials on the official documentations for each project:
-  [Install Docker](https://docs.docker.com/engine/install/)
-  [Install Docker Compose](-  [Install Docker](https://docs.docker.com/engine/install/)
)
To make sure the installations were successful, try running the commands on your terminal.
```bash
docker --version
docker-compose --version
```

### Deploying Apache Pyspark locally
Next, we need to create a `docker-compose.yaml` file with all the configurations needed to a local deploy.
