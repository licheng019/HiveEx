Project 1: Analysis the log come from ibeifeng.com

## Based on the Log format. create the table as below. (run below code in hive command)

drop table if exists default.test_log ;

create table IF NOT EXISTS default.test_log (

remote_addr string,

remote_user string,

time_local string,

request string,

status string,

body_bytes_sent string,

request_body string,

http_referer string,

http_user_agent string,

http_x_forwarded_for string,

host string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'

WITH SERDEPROPERTIES (

  "input.regex" = "(\"[^ ]*\") (\"-|[^ ]*\") (\"[^\]]*\") (\"[^\"]*\") (\"[0-9]*\") (\"[0-9]*\") (-|[^ ]*) (\"[^ ]*\") (\"[^\"]*\") (-|[^ ]*) (\"[^ ]*\")"
)
STORED AS TEXTFILE;

## Import data from local file
load data local inpath '/opt/datas/moodle.ibeifeng.access.log' into table default.test_log
http://www.regexpal.com/(This is the website to verify whether the regex is working)

## Create sub table based on different requirement

a. Business Requirment--which hour has the most person who visit the website.

drop table if exists default.test_log_comm ;

create table IF NOT EXISTS default.test_log_comm (

remote_addr string,

time_local string,

request string,

http_referer string

)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'

STORED AS orc tblproperties ("orc.compress"="SNAPPY");

insert into table default.test_log_comm select remote_addr, time_local, request,http_referer from  default.test_log;

## Define udf to clean some data

#### a. first udf, remove the quote for all the data

#### b. second udf, change the date format from  31/Aug/2015:00:04:37 +0800 to 20150831000437

add jar /opt/datas/removequoteudf.jar ;

add jar /opt/datas/dateTransformUDF.jar ;

create temporary function my_removequotes as "com.cheng.ex.hive.RemoveQuoteUDF" ;

create temporary function my_dateTransform as "com.cheng.ex.hive.DateTransformUDF" ;

insert overwrite table default.test_log_comm select my_removequotes(remote_addr), my_dateTrandform (my_removequotes(time_local)), my_removequotes(request), my_removequotes(http_referer) from  default.test_log ;

## Based on the business requirment, run below query, to get teh result

select timeslot.hour, count(*) as cnt from (select substring(time_local,9,2) hour from test_log_comm) as timeslot group  by timeslot.hour order by cnt desc;
