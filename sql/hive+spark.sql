-- ods的开发
create database szm;

-- ods_szm_data(hive_sql)建表
drop table if exists ods_szm_data;
create external table ods_szm_data(
	`deal_date` string comment '刷卡日期',
	`close_date` string comment '关闭时间',
	`card_no` string comment '卡号',
	`deal_value` string comment '交易价值',
	`deal_type` string comment '出入站类型："地铁入站"、"地铁出站"',
	`company_name` string comment '地铁线名称',
	`car_no` string comment '地铁列车号',
	`station` string comment '地铁站',
	`conn_mark` string comment '连续标记',
	`deal_money` string comment '实收金额',
	`equ_no` string comment '闸机编号'
) comment '深圳地铁刷卡原始数据'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/szm/ods/ods_szm_data'
tblproperties ('org.compression' = 'snappy');


-- ods_szm_data数据装载
load data inpath '/origin_data/szm_data/*' into table ods_szm_data partition(dt='2024-05-02');



-- dwd建表
-- dwd_fact_szt_in_out_detail(地铁进出站总表)
drop table if exists dwd_fact_szt_in_out_detail;
create external table dwd_fact_szt_in_out_detail(
	`deal_date` string comment '刷卡日期',
	`close_date` string comment '关闭时间',
	`card_no` string comment '卡号',
	`deal_value` decimal(16, 2) comment '交易价值',
	`deal_type` string comment '出入站类型："地铁入站"、"地铁出站"',
	`company_name` string comment '地铁线名称',
	`car_no` string comment '地铁列车号',
	`station` string comment '地铁站',
	`conn_mark` string comment '连续标记',
	`deal_money` decimal(16, 2) comment '实收金额',
	`equ_no` string comment '闸机编号'
) comment '地铁进出站总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/szm/dwd/dwd_fact_szt_in_out_detail'
tblproperties ('orc.compression' = 'snappy');



-- dwd_fact_szt_in_detail(地铁进站事实表)
drop table if exists dwd_fact_szt_in_detail;
create external table dwd_fact_szt_in_detail(
	`deal_date` string comment '刷卡日期',
	`card_no` string comment '卡号',
	`company_name` string comment '地铁线名称',
	`car_no` string comment '地铁列车号',
	`station` string comment '地铁站',
	`equ_no` string comment '闸机编号'
) comment '地铁进站事实表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/szm/dwd/dwd_fact_szt_in_detail'
tblproperties ('orc.compression' = 'snappy');




-- dwd_fact_szt_out_detail(地铁出站事实表)
drop table if exists dwd_fact_szt_out_detail;
create external table dwd_fact_szt_out_detail(
	`deal_date` string comment '刷卡日期',
	`close_date` string comment '关闭时间',
	`card_no` string comment '卡号',
	`deal_value` decimal(16, 2) comment '交易价值',
	`company_name` string comment '地铁线名称',
	`car_no` string comment '地铁列车号',
	`station` string comment '地铁站',
	`conn_mark` string comment '连续标记',
	`deal_money` decimal(16, 2) comment '实收金额',
	`equ_no` string comment '闸机编号'
) comment '地铁出站事实表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/szm/dwd/dwd_fact_szt_out_detail'
tblproperties ('orc.compression' = 'snappy');




-- dwd数据装载
-- dwd_fact_szt_in_out_detail(地铁进出站总表)数据装载
insert overwrite table dwd_fact_szt_in_out_detail partition (dt='2024-05-02')
select
	deal_date,
	close_date,
	card_no,
	cast((cast(deal_value as int)/100) as decimal(16, 2)) deal_value,
	deal_type,
	company_name,
	car_no,
	station,
	conn_mark,
	cast((cast(deal_money as float)/100) as decimal(16, 2)) deal_money,
	equ_no
from ods_szm_data
where dt = '2024-05-02'
	and deal_type <> '巴士'
	and unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') > unix_timestamp('2018-09-01 06:14:00', 'yyyy-MM-dd HH:mm:ss')
	and unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') < unix_timestamp('2018-09-01 23:59:00', 'yyyy-MM-dd HH:mm:ss')
order by deal_date;




-- dwd_fact_szt_in_detail(地铁进站事实表)数据装载
insert overwrite table dwd_fact_szt_in_detail partition (dt = '2024-05-02')
select
    deal_date,
    card_no,
    company_name,
    car_no,
    station,
    equ_no
from dwd_fact_szt_in_out_detail
where dt = '2024-05-02'
	and deal_type = '地铁入站'
order by deal_date;




-- dwd_fact_szt_out_detail(地铁出站事实表)
insert overwrite table dwd_fact_szt_out_detail partition (dt = '2024-05-02')
select
    deal_date,
    close_date, 
    card_no, 
	deal_value,
    company_name, 
    car_no, 
    station, 
    conn_mark, 
	deal_money,
    equ_no 
from dwd_fact_szt_in_out_detail
where dt = '2024-05-02'
	and deal_type = '地铁出站'
order by deal_date;