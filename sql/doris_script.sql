-- Apache Doris表设计
create database szm;


-- dwd_fact_szt_in_out_detail_doris(地铁进出站总表)
drop table if exists szm.dwd_fact_szt_in_out_detail_doris;
create table szm.dwd_fact_szt_in_out_detail_doris(
	`deal_date` varchar(255) comment '刷卡日期',
	`close_date` varchar(255) comment '关闭时间',
	`card_no` varchar(255) comment '卡号',
	`deal_type` varchar(255) comment '出入站类型："地铁入站"、"地铁出站"',
	`company_name` varchar(255) comment '地铁线名称',
	`car_no` varchar(255) comment '地铁列车号',
	`station` varchar(255) comment '地铁站',
	`conn_mark` varchar(255) comment '连续标记',
	`dt` date comment 'hive分区字段',
	`deal_value` decimal(16, 2) comment '交易价值',
	`deal_money` decimal(16, 2) comment '实收金额',
	`equ_no` varchar(255) comment '闸机编号'
)
COMMENT '地铁进出站总表'
ENGINE=OLAP
DUPLICATE KEY(`deal_date`,`close_date`,`card_no`,`deal_type`,`company_name`,`car_no`,`station`,`conn_mark`, `dt`)
DISTRIBUTED BY HASH(`card_no`) BUCKETS 1
PROPERTIES ('replication_num'='1');


-- dwd_fact_szt_in_detail_doris(地铁进站事实表)
drop table if exists szm.dwd_fact_szt_in_detail_doris;
create table szm.dwd_fact_szt_in_detail_doris(
	`deal_date` varchar(255) comment "刷卡日期",
	`card_no` varchar(255) comment "卡号",
	`company_name` varchar(255) comment "地铁线名称",
	`car_no` varchar(255) comment "地铁列车号",
	`station` varchar(255) comment "地铁站",
	`dt` date comment 'hive分区字段',
	`equ_no` varchar(255) comment "闸机编号"
) 
COMMENT "地铁进站事实表"
ENGINE=OLAP
DUPLICATE KEY(`deal_date`, `card_no`, `company_name`, `car_no`, `station`, `dt`)
DISTRIBUTED BY HASH(card_no) BUCKETS 1
PROPERTIES ('replication_num'='1');



-- dwd_fact_szt_out_detail(地铁出站事实表)
drop table if exists szm.dwd_fact_szt_out_detail_doris;
create table dwd_fact_szt_out_detail_doris(
	`deal_date` varchar(255) comment "刷卡日期",
	`close_date` varchar(255) comment "关闭时间",
	`card_no` varchar(255) comment "卡号",
	`company_name` varchar(255) comment "地铁线名称",
	`car_no` varchar(255) comment "地铁列车号",
	`station` varchar(255) comment "地铁站",
	`conn_mark` varchar(255) comment "连续标记",
	`dt` date comment 'hive分区字段',
	`deal_value` decimal(16, 2) comment "交易价值",
	`deal_money` decimal(16, 2) comment "实收金额",
	`equ_no` varchar(255) comment "闸机编号"
) 
COMMENT "地铁出站事实表"
ENGINE=OLAP
DUPLICATE KEY(`deal_date`, `close_date`, `card_no`, `company_name`, `car_no`, `station`, `conn_mark`, `dt`)
DISTRIBUTED BY HASH(card_no) BUCKETS 1
PROPERTIES ('replication_num'='1');




-- doris stream load
-- dwd_fact_szt_in_out_detail_doris 数据装载
curl --location-trusted -u root: \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:deal_date,close_date,card_no,deal_value,deal_type,company_name,car_no,station,conn_mark,deal_money,equ_no,dt" \
    -T part-00000-bb3ffe26-b95d-4bf5-8758-3c0d1eecbc3a-c000.csv \
    -XPUT http://172.20.10.3:8070/api/szm/dwd_fact_szt_in_out_detail_doris/_stream_load

curl --location-trusted -u root: \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:deal_date,card_no,company_name,car_no,station,equ_no,dt" \
    -T part-00000-089eba11-ed8f-41e7-b9a6-15ac7b9131e2-c000.csv \
    -XPUT http://172.20.10.3:8070/api/szm/dwd_fact_szt_in_detail_doris/_stream_load

curl --location-trusted -u root: \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:deal_date,close_date,card_no,deal_value,company_name,car_no,station,conn_mark,deal_money,equ_no,dt" \
    -T part-00000-a27c3213-789b-4326-9100-57c4a9ce3f1d-c000.csv \
    -XPUT http://172.20.10.3:8070/api/szm/dwd_fact_szt_out_detail_doris/_stream_load




-- ADS设计与开发
-- **ads_in_station_day_top(每站进站人次排行榜)**
drop table if exists szm.ads_in_station_day_top;
create table szm.ads_in_station_day_top(
	`station` varchar(50) comment "站名",
	`people_numbers` int sum default "0" comment "人次"
)
AGGREGATE KEY(`station`)
DISTRIBUTED BY HASH(`station`) BUCKETS 1
PROPERTIES (
	"replication_num" = "1"
);

-- 数据装载
insert into szm.ads_in_station_day_top
select
	station,
	count(1) ct
from dwd_fact_szt_in_detail_doris
where dt = '2024-05-02'
group by station

-- 数据查询
select
	*
from szm.ads_in_station_day_top
order by people_numbers desc
limit 20;



-- **ads_out_station_day_top(每站出站人次排行榜)**
drop table if exists szm.ads_out_station_day_top;
create table szm.ads_out_station_day_top(
	`station` varchar(50) comment "站名",
	`people_numbers` int sum default "0" comment "人次"
)
aggregate key(`station`)
distributed by hash(`station`) buckets 1
properties (
	"replication_num" = "1"
);


insert into szm.ads_out_station_day_top
select
	station,
	count(1) ct
from dwd_fact_szt_out_detail_doris
where dt = '2024-05-02'
group by station


select
	*
from ads_out_station_day_top
order by people_numbers desc
limit 20;


-- ads_in_out_station_day_top(每站进出站人次排行榜)
drop table if exists szm.ads_in_out_station_day_top;
create table szm.ads_in_out_station_day_top(
	`station` varchar(50) comment "站名",
	`people_numbers` int sum default "0" comment "人次"
)
aggregate key(`station`)
distributed by hash(`station`) buckets 1
properties (
	"replication_num" = "1"
);


insert into szm.ads_in_out_station_day_top
select
	station,
	count(1) ct
from szm.dwd_fact_szt_in_out_detail_doris
where dt = '2024-05-02'
group by station;



-- **ads_card_deal_day_top(每卡日消费排行)**
drop table if exists szm.ads_card_deal_day_top;
create table szm.ads_card_deal_day_top(
	`card_no` varchar(50) comment "卡号",
	`dt` date comment "每日分区号",
	`deal_value_sum` decimal(16, 2) sum default "0.00" comment "消费总额"
)
aggregate key(`card_no`, `dt`)
distributed by hash(`card_no`) buckets 1
properties (
	"replication_num" = "1"
);


insert into szm.ads_card_deal_day_top
select
	card_no,
	dt,
	deal_value
from dwd_fact_szt_out_detail_doris
where dt = '2024-05-02';



-- **ads_line_send_passengers_day_top(每线路单日运输乘客总次数排行榜，进站算一次，出站并且联程算一次)**
drop table if exists ads_line_send_passengers_day_top;
create table ads_line_send_passengers_day_top(
	`company_name` varchar(50) comment "地铁线路号",
	`dt` date comment "每日分区号",
	`passengers_number_total` int sum default "0" comment "乘客总次数"
)
aggregate key(`company_name`, `dt`)
distributed by hash(`company_name`) buckets 1
properties(
	"replication_num" = "1"
);



insert into szm.ads_line_send_passengers_day_top
SELECT
	company_name,
	dt,
	tag
from (
SELECT 
	company_name,
	dt,
	1 tag
from dwd_fact_szt_in_out_detail_doris
where deal_type = '地铁入站' 
	and dt = '2024-05-02'
union all
select
	company_name,
	dt,
	1 tag
from dwd_fact_szt_in_out_detail_doris
where deal_type = '地铁出站' 
	and conn_mark = '1'
	and dt = '2024-05-02'
) tb1;



-- **ads_stations_send_passengers_day_top(每日运输乘客最多的车站区间排行榜)** 
drop table if exists szm.ads_stations_send_passengers_day_top;
create table szm.ads_stations_send_passengers_day_top(
	`station_interval` varchar(50) comment "车站区间",
	`dt` date comment "每日分区号",
	`passengers_number_total` int sum comment "乘客总次数"
)
aggregate key(`station_interval`, `dt`)
distributed by hash(`station_interval`) buckets 1
properties (
	"replication_num" = "1"
);




insert into ads_stations_send_passengers_day_top
select
	CONCAT_WS('>', SUBSTR(deal_type_station, 6), SUBSTR(deal_type_station2, 6)) station_interval,
	dt,
	1
from (
	select
		card_no,
		deal_date,
		deal_type,
		company_name,
		dt,
		station,
		deal_type_station,
		lead(deal_type_station, 1, null) over(partition by card_no order by deal_date) deal_type_station2
	from (
		select
			card_no,
			deal_date,
			deal_type,
			company_name,
			dt,
			station,
			CONCAT_WS('@', deal_type, station) deal_type_station 
		from (
			select
				card_no,
				deal_date,
				conn_mark,
				deal_type,
				company_name,
				dt,
				station
			from dwd_fact_szt_in_out_detail_doris
			order by card_no, deal_date
			) tb1
		) tb2
	) tb3
where SUBSTR(deal_type_station, 1, 4) = '地铁入站' and SUBSTR(deal_type_station2, 1, 4) = '地铁出站';