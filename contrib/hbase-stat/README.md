# hbase-stat
=============

A simple statistics package for HBase.

## Goal
========

Provide reasonable approximations (exact, if we can manage it) of statistical information about a table in HBase with minimal muss and fuss.

We want to make it easy to gather statistics about your HBase tables - there should be little to no work on the users part beyond ensuring that the right things are setup.

## Usage
=========

There is a single 'system' table called '_stats'. When creating your table, you should use the ```SetupTableUtil``` to ensure that:

1. The table you are creating has the right description (e.g. includes the right coprocessors)
2. A statistics table exists to match the table you are creating

For example, to create a table called 'primary' with the MinMaxKey statistic enabled and also create the statistics table, you would do:

```java
    HTableDescriptor primary = new HTableDescriptor("primary");
    primary.addFamily(new HColumnDescriptor(FAM));
    
    //add the min/max key stats
    MinMaxKey.addToTable(primary);

    // setup the stats table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    SetupTableUtil.setupTable(admin, primary, true);
```


### Statistics Table Schema
===========================

The schema was inspired by OpenTSDB (opentsdb.net) where each statistic is first grouped by table, then region. After that, each statistic (MetricValue) is grouped by:
	* type
	* info
	** this is like the sub-type of the metric to help describe the actual type. For instance, on a min/max for the column, this could be 'min'
	* value

Suppose that we have a table called 'primary' with column 'col' and we are using the MinMaxKey statistic. Assuming the table has a single region, entries in the statistics table will look something like:

```
|           Row             | Column Family | Column Qualifier | Value 
|  primary<region name>col  |     STAT      |   max_region_key |  10  
|  primary<region name>col  |     STAT      |   min_region_key |  3
```

This is because the MinMaxKey statistic uses the column name (in this case 'col') as the type, we use the only CF on the stats table (STATS) and have to subtypes - info - elements: max_region_key and min_region_key, each with associated values.

## Requirements
===============

* Java 1.6.0_34 or higher
* HBase-0.94.5 or higher

### If building from source
* Maven 3.X


## Building from source
=======================

To run tests

    $ mvn -o clean test
    
To build a jar

    $ mvn clean package
