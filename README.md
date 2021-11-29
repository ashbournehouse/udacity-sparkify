# README for udacity-sparkify

This repository contains my submission for the 2nd Project in Udacity's Data Engineering nanodegree.

The default way to submit this project is using online Jupyter workbooks. In order to learn more
the basic set-up and configuration of Apache Cassandra, I have chosen to run a local, single-node,
Cassandra installation and connect to that using a standard Python code file.

***
> In the code below the address of the Cassandra node is substituted with "aaa.bb.ccc.ddd". If you
> are running this code with your own stand-alone server you will need to substitute in the correct
> IP address.
***

I have used the Python logger in an attempt to make the code more readable than when using
'debug print' statements.


## Data Modelling wih Apache Cassandra

***
> *This is a submission for the Udacity Data Engineering 'nanodegree' project entitled
>  **'Data Modelling with Apache Cassandra'.***
***

The task or challenge comes in two parts

## Part 1 - ETL pipeline for pre-processing the supplied csv file

The source data for the project is supplied as a folder containing a number of csv
files and we are asked to pre-process these to create a single csv file containg all
the required data.

In this process, unwanted lines from the supplied data files that do not refer to
'song plays' are discarded.

## Part 2 - Transfer the data to Apache Cassandra and run some queries

The required sequence of tasks goes something like this:

* Connect to Cassandra
* Set-up a cluster and keyspace
* For each of 3 queries:
    * Create a table that is suitable for that query
    * Populate the table from the csv data created in Part 1
    * Run the query
    * Clean up




