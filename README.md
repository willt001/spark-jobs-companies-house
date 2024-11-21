# Spark Jobs with Companies House dataset

The aim of this repository is to answer potential business questions about UK companies using PySpark, while showing a method for developing jobs locally and then running on AWS Glue.

The datasets for this repo can be downloaded using the below Powershell commands. Data is approx 1 GB as .zip and 7 GB uncompressed.

```bash
curl https://www.kaggle.com/api/v1/datasets/download/rzykov/uk-corporate-data-company-house-2023 -o corporate_uk.zip

Expand-Archive corporate_uk.zip ./
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
History server (optional), the history server can also be run individaully to parse logs from Spark Local mode:
```bash
cd $Env:SPARK_HOME; spark-class org.apache.spark.deploy.history.HistoryServer
```

## Spark Job Performance Optimisation

In total, the size of all the input files is 6747 MB. Since the default value for ```spark.sql.files.maxPartitionBytes``` is 128 MB, there will be 53 partitions when all four datasets are read. 

To maximise parallelism while also minimising CPU idle time after shuffles I would choose approximately 53 / 2 total cores for the Spark cluster. Since Spark performance has been shown to bottleneck when using more than four cores per executor, I would choose to use 6 executors with 4 cores each for this Spark application.

As an example, I have testing running the Spark job ```local_jobs.py``` in Spark local mode with both 4 and 1 cores. 4 cores took 58 seconds and 1 core took 2.4 minutes.

![1v4cores](https://github.com/user-attachments/assets/9931f292-a485-4123-95c5-853c7e3a6797)

When investigating the event timelines for reading one of the files, clearly running with 4 cores benefits job performance as Spark is able to to treat CSV as a splittable file format and read it in parallel from multiple cores.

1 core:
![1 core](https://github.com/user-attachments/assets/610f0309-a7b5-4f9a-b288-65f8b15973a1)

4 cores:
![4 cores](https://github.com/user-attachments/assets/7a887ac9-475a-41cd-8946-893eec062a31)

## Running the jobs on AWS Glue

I have added a slightly modified version of ```local_jobs.py``` called ```glue_jobs.py```. This script can be imported into AWS Glue Studio and accepts ```BUCKET_NAME``` as a parameter where the input files will be stored and output files written.

As per the performance optimisation, I am using 6 of the G 1X workers (4 cores and 16 GB RAM each) to run this job in Glue.


