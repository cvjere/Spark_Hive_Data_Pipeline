
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;




CREATE EXTERNAL TABLE if not exists shekhardb.hadoopproject1 (MSA STRING, POP STRING, year string, month string, totalRainfall double, WettestPopulation double) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' LOCATION 'hdfs://quickstart.cloudera:8020/user/cloudera/hadoopproject/output/wettestpopulation';

-- Dynamic parition
CREATE TABLE IF NOT EXISTS shekhardb.hadoopproject2 (MSA STRING, POP STRING,  totalRainfall double, WettestPopulation double) PARTITIONED BY (year string, month string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED as PARQUET;
INSERT into TABLE shekhardb.hadoopproject2 PARTITION (year, month ) SELECT MSA,POP,totalRainfall,WettestPopulation, year, month FROM shekhardb.hadoopproject1 


