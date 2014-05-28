Introduction
------------

This project is an example of computing the classic WordCount from a
corpus of [Apache Avro](http://avro.apache.org/)-encoded records using Spark. There are examples using the Avro Specific and Avro Generic records.




Spark
--------

Requirements
============

* Java 1.7+ 
* sbt
//* Hadoop (tested with Hadoop 1.2.1) installed and `hadoop` on the `PATH`.

Reading Avro Files
==================
Run in local mode with `$ sbt run` and choose to run either the `GenericRecord` example or the `SpecificRecord` example.

To run on your cluster, uncomment the extra arguments of `SparkContext`, and generate a fat jar with `sbt assembly` (WARNING: THE `assembly` TASK FAILS FOR ME, AS THERE ARE MANY 'DUPLICATE DEPENDENCY' ERRORS - MAYBE USING MAVEN TO PACKAGE WILL SOLVE THE PROBLEM?)


Writing Avro Files
==================

//TODO: Currently failing to write. Why? Maybe improved support in Spark 1.0? (see https://github.com/sryza/simplesparkavroapp)


