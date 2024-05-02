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
	`statiion` string comment '地铁站',
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