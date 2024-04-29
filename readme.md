# 项目简介

本项目主要面对的数据是深圳地铁刷卡数据，通过Spark + Hive等大数据技术分析海量的深圳地铁刷卡数据，来探究深圳地铁客运能力，探索深圳地铁运营服务、客流情况，为深圳地铁优化运营服务提供支撑；

本项目构建了一个智慧、高效、多维的数据仓库，将深圳地铁刷卡数据，为深圳地铁数据分析、数据运营决策提供有效数据的支撑；

# 深圳地铁客流分析架构

## 技术架构：

Hadoop HDFS：存储海量深圳地铁刷卡数据；

Spark on Hive：Hive MetaStore存储以及提高元数据服务，Spark利用海量数据处理、计算的能力；

Doris：OLAP引擎，进行快速、高效的统计指标计算；

Poweri BI：提高数据可视化报表服务；

## 数据仓库架构：

数据仓库：使用HDFS + Hive + Spark + Doris + Power BI构建整体的数据仓库，如下图所示：

![数据仓库架构图](..\SZM_PF_BG_Analysis\drawing_bed\数据仓库架构图.png)



# 指标需求：

```txt
【体现进站压力】 每站进站人次排行榜      
	ads_in_station_day_top
【体现出站压力】 每站出站人次排行榜      
	ads_out_station_day_top
【体现进出站压力】 每站进出站人次排行榜      
	ads_in_out_station_day_top
【体现通勤车费最多】 每卡日消费排行      
	ads_card_deal_day_top  
【体现线路运输贡献度】 每线路单日运输乘客总次数排行榜，进站算一次，出站并且联程算一次     
	ads_line_send_passengers_day_top  
【体现利用率最高的车站区间】 每日运输乘客最多的车站区间排行榜       
	ads_stations_send_passengers_day_top
【体现线路的平均通勤时间，运输效率】 每条线路单程直达乘客耗时平均值排行榜     
	ads_line_single_ride_average_time_day_top
【体现深圳地铁全市乘客平均通勤时间】 所有乘客从上车到下车间隔时间平均值    
	ads_all_passengers_single_ride_spend_time_average
【体现通勤时间最长的乘客】 单日从上车到下车间隔时间排行榜     
	ads_passenger_spend_time_day_top
【体现车站配置】 每个站点进出站闸机数量排行榜
	每个站点入站闸机数量  		ads_station_in_equ_num_top
	每个站点出站闸机数量    		ads_station_out_equ_num_top
【体现各线路综合服务水平】 各线路进出站闸机数排行榜
	各线路进站闸机数排行榜 		ads_line_in_equ_num_top.png
	各线路出站闸机数排行榜 		ads_line_out_equ_num_top
【体现收入最多的车站】 出站交易收入排行榜   
	ads_station_deal_day_top
【体现收入最多的线路】 出站交易所在线路收入排行榜   
	ads_line_deal_day_top
【体现换乘比例、乘车体验】 每天每线路换乘出站乘客百分比排行榜  
	ads_conn_ratio_day_top
【体现每条线的深圳通乘车卡普及程度 9.5 折优惠】 出站交易优惠人数百分比排行榜     
	ads_line_sale_ratio_top
【体现换乘的心酸】 换乘耗时最久的乘客排行榜	
	ads_conn_spend_time_top
【体现线路拥挤程度】 上车以后还没下车，每分钟、小时每条线在线人数   
	ads_on_line_min_top
```

