# Spark Jobs with Companies House dataset

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
