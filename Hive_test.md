https://github.com/adaltas/esgf-2020-fall-bigdata/blob/master/modules/05.hive/lab.md

correction: https://github.com/adaltas/esgf-2020-fall-bigdata/blob/corrections/modules/05.hive/lab.md

Code
--------------

use esgf_2020_fall_1;
SET hivevar:username=yidrissi;

Q2:

CREATE EXTERNAL TABLE IF NOT EXISTS esgf_2020_fall_1.${username}_drivers_ext (
  driver_id INT,
  name VARCHAR(10),
  ssn INT,
  location VARCHAR(50),
  certified CHAR(1),
  wageplan VARCHAR(5)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 'drivers/'
TBLPROPERTIES ('skip.header.line.count'='1');

Q3:

CREATE TABLE IF NOT EXISTS esgf_2020_fall_1.${username}_drivers (
  driver_id INT,
  first_name VARCHAR(10),
  last_name VARCHAR(10),
  ssn INT,
  address VARCHAR(50),
  certified BOOLEAN,
  wageplan VARCHAR(5)
)
STORED AS ORC;

Q4:

INSERT OVERWRITE TABLE esgf_2020_fall_1.${username}_drivers
SELECT 
  driver_id,
  substring_index(name, ' ', 1),
  substring_index(name, ' ', -1),
  ssn,
  location,
  if(certified='Y', true, false),
  wageplan
FROM esgf_2020_fall_1.${username}_drivers_ext;

Bonus 1 & 2:

SELECT COUNT(certified) from yidrissi_drivers where certified = True;

32

SELECT max(ssn) from yidrissi_drivers;

977706052

Bonus 3:

 Play with a bigger data sets. Chose the most interesting for you here "hdfs dfs -ls /data/" and make the ORC table out of it.

Bonus 4:

 Explore created ORC file and compare it with the original CSV (file size for example)




