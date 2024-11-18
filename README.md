# Spark Jobs with Companies House dataset

The aim of this repository is to answer potential business questions about UK companies using PySpark, while showing a method for developing jobs locally and then running on AWS Glue.

The datasets for this repo can be downloaded using the below Powershell commands. Data is approx 1 GB as .zip and 7 GB uncompressed.

```bash
curl https://www.kaggle.com/api/v1/datasets/download/rzykov/uk-corporate-data-company-house-2023 -o corporate_uk.zip

Expand-Archive corporate_uk.zip ~/
```

## Developing and running Spark jobs locally using PySpark

There are two main methods for running spark locally, Spark Local Mode and Spark Standalone. Both will require Python, Java, Apache Spark, and PySpark to be installed with environment variables for SPARK_HOME, JAVA_HOME, and PATH to be configured.

### Spark Local Mode

Spark Local Mode is a configuration where all of Spark runs inside of a single JVM, the user defines the number of cores used by the JVM with ```local[n]```.

### Spark Standalone

Spark Standalone is a configuration where you can create multiple JVMs to form a local Spark cluster: one master node, one or more worker nodes, and optionally a Spark history server. This configuration uses Spark's built in cluster manager.

To setup a Spark Standalone cluster with Powershell, run each of the below in seperate terminals:

Master node:
```bash
cd $Env:SPARK_HOME; spark-class org.apache.spark.deploy.master.Master
```
Worker node (replace <spark_url> with the url of your master node), -c specifies number of cores, -m specific memory (2G = 2 GB):
```bash
cd $Env:SPARK_HOME; spark-class org.apache.spark.deploy.worker.Worker -c 2 -m 2G <spark_url>
```
History server (optional):
```bash
cd $Env:SPARK_HOME; spark-class org.apache.spark.deploy.history.HistoryServer
```

## Spark Job Performance Optimisation

The largest of the four datasets is ```filings.csv``` (2042 MB). Since csv files are splittable by Spark with a default maximum block size of 128 MB, to maximise parallelism we should use 16 CPU cores (2042 / 128 = 15.95).

For example, reading the ```filings.csv``` data using 4 cores ```spark = SparkSession.builder.master('local[4]').getOrCreate()``` allows 4 data blocks to be read simultaenously and the total job duration is 14 seconds.

Using just 2 cores would halve the parallelism and job duration increases to 21 seconds.

2 cores: ![2 cores](https://github.com/user-attachments/assets/9b6457a7-5fc2-4eb0-886f-d37289a9424d)

4 cores: ![4 cores](https://github.com/user-attachments/assets/cd9b562e-e93d-46f8-8fba-c866e86b4b95)
