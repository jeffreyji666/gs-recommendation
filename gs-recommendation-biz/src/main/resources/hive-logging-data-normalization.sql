/* hive -f hive-logging-data-normalization.sql */
create database if not exists gs_recommendation location '/user/you/hive';

use gs_recommendation;

create table if not exists user_logging(
	productid int,
	vid string comment 'user cookie string, 20 char length',
	start string comment 'the start time of user accessing the page',
	duration string comment 'the duration which user stay at the page',
	domain string comment 'domain of current page',
	userid string,
	signedin boolean,
	usergrade tinyint comment 'user ranking',
	startcitypkg smallint comment 'the start place of vocation product',
	orderid int
)row format delimited fields terminated by '\073';

load data inpath '/user/you/logging/logging' overwrite into table user_logging;

create table if not exists user_bind(
	userid int,
	bindusername string comment 'user name which related to user_logging userid',
	bindtypeid tinyint
) row format delimited fields terminated by ',';

load data inpath '/user/you/logging/bind' overwrite into table user_bind;

drop table if exists user_rating;

create table user_rating
	row format serde "org.apache.hadoop.hive.serde2.columnar.columnarserde"
	stored as textfile
	as select bind.userid,logging.productid,count(*) 
	from (select * from user_logging where user_logging.signedin=true and user_logging.orderid=0)logging 
	join user_bind bind
	on (logging.userid = bind.bindusername) 
	group by bind.userid,logging.productid;