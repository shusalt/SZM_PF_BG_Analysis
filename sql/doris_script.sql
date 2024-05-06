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



-- ads_line_single_ride_average_time_day_top(每条线路单程直达乘客耗时平均值排行榜)
drop table if exists szm.ads_line_single_ride_average_time_day_top;
create table szm.ads_line_single_ride_average_time_day_top(
	`company_name` varchar(50) comment "地铁线路",
	`dt` date comment "每日分区号",
	`avg_riding_time` int comment "平均乘坐时间"
)
duplicate key(`company_name`, `dt`)
distributed by hash(`company_name`) buckets 1
properties (
	"replication_num" = "1"
);




insert into szm.ads_line_single_ride_average_time_day_top
select
	company_name,
	dt,
	avg(riding_time) avg_riding_time
from (
	select
		-- 筛选出单程的记录
		company_name,
		dt,
		-- 计算单程直达乘坐时间
		round((unix_timestamp(deal_date2) - unix_timestamp(deal_date)) / 60) riding_time
	from (
		-- 选取下一个记录的时间、出入站类型，为计算单程做准备
		select
			card_no,
			deal_date,
			-- 去下一个记录的时间
			lead(deal_date, 1, null) over(partition by card_no order by deal_date) deal_date2,
			conn_mark,
			deal_type,
			company_name,
			dt,
			station,
			deal_type_company,
			-- 去下一个记录的deal_type_company
			lead(deal_type_company, 1, null) over(partition by card_no order by deal_date) deal_type_company2
		from (
			-- 拼接: 出入站类型+地铁线路
			select
				card_no,
				deal_date,
				conn_mark,
				deal_type,
				company_name,
				dt,
				station,
				concat_ws('@', deal_type, company_name) deal_type_company
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
	-- 筛选出单程直达的数据
	where SUBSTR(deal_type_company, 1, 4) = '地铁入站' 
		and SUBSTR(deal_type_company2, 1, 4) = '地铁出站'
		and conn_mark = '0'
	order by card_no, deal_date	
) tb4
group by company_name, dt



-- ads_all_passengers_single_ride_spend_time_average(所有乘客从上车到下车间隔时间平均值)
drop table if exists szm.ads_all_passengers_single_ride_spend_time_average;
create table ads_all_passengers_single_ride_spend_time_average(
	`dt` date comment "每日分区号",
	`avg_interval_time` int replace_if_not_null default "0" comment "间隔时间平均值"
)
aggregate keu(`dt`)
distributed by hash(`avg_interval_time`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_all_passengers_single_ride_spend_time_average
select
	dt,
	round(avg(riding_time)) avg_riding_time
from (
	select
		dt,
		round((unix_timestamp(deal_date2) - unix_timestamp(deal_date)) / 60) riding_time
	from (
		-- 选取下一个记录的时间、出入站类型，为计算单程做准备
		select
			card_no,
			deal_date,
			-- 去下一个记录的时间
			lead(deal_date, 1, null) over(partition by card_no order by deal_date) deal_date2,
			conn_mark,
			deal_type,
			company_name,
			dt,
			station,
			deal_type_company,
			-- 去下一个记录的deal_type_company
			lead(deal_type_company, 1, null) over(partition by card_no order by deal_date) deal_type_company2
		from (
			-- 拼接: 出入站类型+地铁线路
			select
				card_no,
				deal_date,
				conn_mark,
				deal_type,
				company_name,
				dt,
				station,
				concat_ws('@', deal_type, company_name) deal_type_company
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
	-- 筛选出单程直达的数据
	where SUBSTR(deal_type_company, 1, 4) = '地铁入站' 
		and SUBSTR(deal_type_company2, 1, 4) = '地铁出站'
	order by card_no, deal_date	
) tb4
group by dt;


select * from ads_all_passengers_single_ride_spend_time_average


-- ads_passenger_spend_time_day_top(单日从上车到下车间隔时间排行榜)
drop table if exists ads_passenger_spend_time_day_top;
create table ads_passenger_spend_time_day_top(
	`card_no` varchar(50) comment "卡号",
	`dt` varchar(50) comment "每日分区号",
	`max_riding_time` int max default "0" comment "最大乘坐时间"
)
aggregate key(`card_no`, `dt`)
distributed by hash(`card_no`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_passenger_spend_time_day_top
select
	-- 筛选出单程的记录
	card_no,
	dt,
	-- 计算单程直达乘坐时间
	round((unix_timestamp(deal_date2) - unix_timestamp(deal_date)) / 60) riding_time
from (
	-- 选取下一个记录的时间、出入站类型，为计算单程做准备
	select
		card_no,
		deal_date,
		-- 去下一个记录的时间
		lead(deal_date, 1, null) over(partition by card_no order by deal_date) deal_date2,
		conn_mark,
		deal_type,
		company_name,
		dt,
		station,
		deal_type_company,
		-- 去下一个记录的deal_type_company
		lead(deal_type_company, 1, null) over(partition by card_no order by deal_date) deal_type_company2
	from (
		-- 拼接: 出入站类型+地铁线路
		select
			card_no,
			deal_date,
			conn_mark,
			deal_type,
			company_name,
			dt,
			station,
			concat_ws('@', deal_type, company_name) deal_type_company
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
-- 筛选出单程直达的数据
where SUBSTR(deal_type_company, 1, 4) = '地铁入站' 
	and SUBSTR(deal_type_company2, 1, 4) = '地铁出站';




-- ads_station_in_equ_num(进站主题)
drop table if exists ads_station_in_equ_num;
create table ads_station_in_equ_num(
	`company_name` varchar(50) comment "地铁线名",
	`station` varchar(50) comment "站名",
	`dt` date comment "每日分区字段",
	`equ_no_num` int comment "闸机数"
)
duplicate key(`company_name`, `station`, `dt`)
distributed by hash(`company_name`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_station_in_equ_num
select
	company_name,
	station,
	dt,
	count(distinct equ_no) equ_no_num
from dwd_fact_szt_in_detail_doris dfsidd 
group by company_name, station, dt



-- 每个站点入站闸机数量  ads_station_in_equ_num_top:
select
	station,
	equ_no_num
from ads_station_in_equ_num
order by equ_no_num desc
limit 10;



-- 各线路进站闸机数统计排行榜 ads_line_in_equ_num_top
select
	company_name,
	sum(equ_no_num) equ_no_num
from ads_station_in_equ_num
group by company_name
order by equ_no_num desc
limit 10;



-- ads_station_out_equ_num(出站主题)
drop table if exists ads_station_out_equ_num
create table ads_station_out_equ_num(
	`company_name` varchar(50) comment "地铁线名",
	`station` varchar(50) comment "站名",
	`dt` date comment "每日分区字段",
	`equ_no_num` int comment "闸机数"
)
duplicate key(`company_name`, `station`, `dt`)
distributed by hash(`company_name`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_station_out_equ_num
select
	company_name,
	station,
	dt,
	count(distinct equ_no) equ_no_num
from dwd_fact_szt_out_detail_doris
group by company_name, station, dt;


-- 每个站点出站闸机数量 ads_station_out_equ_num_top:
select
	station,
	equ_no_num
from ads_station_out_equ_num
order by equ_no_num desc
limit 10;



-- 各线路出站闸机数排行榜 ads_line_out_equ_num_top:
select
	company_name,
	sum(equ_no_num) equ_no_num
from ads_station_out_equ_num
group by company_name
order by equ_no_num desc
limit 10;




-- ads_deal_day_top(各线路与站点收益统计表)
drop table if exists ads_company_station_deal_day_top;
create table ads_company_station_deal_day_top(
	`company_name` varchar(50) comment "地铁线路",
	`station` varchar(50) comment "站点",
	`dt` date comment "每日分区号",
	`total_deal_value` decimal(16, 2) sum default "0" comment "总收益",
	`total_deal_money` decimal(16, 2) sum default "0" comment "折后总收益"
)
aggregate key(`company_name`, `station`, `dt`)
distributed by hash(`company_name`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_company_station_deal_day_top
select
	company_name,
	station,
	dt,
	deal_value,
	deal_money
from dwd_fact_szt_out_detail_doris;



select
	station,
	total_deal_value,
	total_deal_money
from ads_company_station_deal_day_top
order by total_deal_value desc
limit 10;




select
	company_name,
	sum(total_deal_value) total_deal_value,
	sum(total_deal_money) deal_money
from ads_company_station_deal_day_top
group by company_name
order by total_deal_value desc
limit 10;




-- ads_line_sale_ratio_top(深圳地铁各线路直达乘客优惠人次百分比排行榜)
drop table if exists ads_line_sale_ratio_top;
create table ads_line_sale_ratio_top(
	`company_name` varchar(50) comment "地铁线路",
	`dt` varchar(50) comment "每日分区号",
	`percentage` float comment "优惠人次百分比"
)
duplicate key(`company_name`, `dt`)
distributed by hash(`company_name`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_line_sale_ratio_top
select
	tb1.company_name,
	tb1.dt,
	round((ct2/ct1) * 100, 2) percentage
from (
	-- 各线路全部乘客数
	select
		company_name,
		dt,
		count(1) ct1
	from dwd_fact_szt_out_detail_doris dfsodd
	group by company_name, dt
) tb1
join (
	-- 各个线路使用深圳通地铁卡优惠的乘客数
	select
		company_name,
		dt,
		count(1) ct2
	from dwd_fact_szt_out_detail_doris dfsodd 
	where conn_mark = '0' and deal_money != 0
	group by company_name, dt
) tb2
on tb1.company_name = tb2.company_name and tb1.dt = tb2.dt;



select
	*
from ads_line_sale_ratio_top
order by percentage desc



-- ads_conn_ratio_day_top(深圳地铁各线路换乘出站乘客百分比排行榜)
drop table if exists ads_conn_ratio_day_top;
create table ads_conn_ratio_day_top(
	`company_name` varchar(50) comment "地铁线路",
	`dt` varchar(50) comment "每日分区号",
	`percentage` float comment "换乘出站乘客百分比"
)
duplicate key(`company_name`, `dt`)
distributed by hash(`company_name`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_conn_ratio_day_top
select
	tb1.company_name,
	tb1.dt,
	round(ct2/ct1 * 100, 2)percentage
from (
	-- 各线路全部乘客数
	select
		company_name,
		dt,
		count(1) ct1
	from dwd_fact_szt_out_detail_doris dfsodd
	group by company_name, dt
) tb1
join (
	-- 各线路换乘出站乘客数
	select
		company_name,
		dt,
		count(1) ct2
	from dwd_fact_szt_out_detail_doris dfsodd 
	where conn_mark = '1'
	group by company_name, dt
) tb2
on tb1.company_name = tb2.company_name and tb1.dt = tb2.dt;


select
	*
from ads_conn_ratio_day_top
order by percentage desc;




-- ads_conn_spend_time_top(深圳地铁换乘时间最久的乘客排行榜)
drop table if exists ads_conn_spend_time_top;
create table ads_conn_spend_time_top(
	`card_no` varchar(50) comment "卡号", 
	`dt` date comment "每日分区号",
	`riding_time` int max default "0" comment "最久换乘时间"
)
aggregate key(`card_no`, `dt`)
distributed by hash(`card_no`) buckets 1
properties (
	"replication_num" = "1"
);



insert into ads_conn_spend_time_top
select
	-- 筛选出单程的记录
	card_no,
	dt,
	-- 计算单程直达乘坐时间
	round((unix_timestamp(deal_date2) - unix_timestamp(deal_date)) / 60) riding_time
from (
	-- 选取下一个记录的时间、出入站类型，为计算单程做准备
	select
		card_no,
		deal_date,
		-- 去下一个记录的时间
		lead(deal_date, 1, null) over(partition by card_no order by deal_date) deal_date2,
		conn_mark,
		deal_type,
		company_name,
		dt,
		station,
		deal_type_company_station,
		-- 去下一个记录的deal_type_company
		lead(deal_type_company_station, 1, null) over(partition by card_no order by deal_date) deal_type_company_station2
	from (
		-- 拼接: 出入站类型+地铁线路
		select
			card_no,
			deal_date,
			conn_mark,
			deal_type,
			company_name,
			dt,
			station,
			concat_ws('@', deal_type, company_name, station) deal_type_company_station
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
-- 筛选出单程直达的数据
where SUBSTR(deal_type_company_station, 1, 4) = '地铁出站' 
	and SUBSTR(deal_type_company_station2, 1, 4) = '地铁入站'
	and conn_mark = '1'
	and substr(deal_type_company_station,12) != substr(deal_type_company_station2,12)