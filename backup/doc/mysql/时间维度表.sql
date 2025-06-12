# time span
SET @d0 = "2012-01-01";
SET @d1 = "2019-01-01";
SET @date = date_sub(@d0, interval 1 day);
set @y=null;
set @report_w=0;
set @bz=0;
# set up the time dimension table
DROP TABLE IF EXISTS dim_time;
CREATE TABLE `dim_time` (
  `date` date DEFAULT NULL,
  `id` int NOT NULL,
  `y` smallint DEFAULT NULL,
  `m` smallint DEFAULT NULL,
  `d` smallint DEFAULT NULL,
  `yw` smallint DEFAULT NULL,
  `w` smallint DEFAULT NULL,
  `q` smallint DEFAULT NULL,
  `wd` smallint DEFAULT NULL,
  `m_name`  char(10) DEFAULT NULL,
  `wd_name` char(10) DEFAULT NULL,
  `report_w` int(10) DEFAULT NULL,
  `bz` int(10),
  PRIMARY KEY (`id`));
# populate the table with dates
INSERT INTO dim_time(date,id,bz,report_w,y,m,d,yw,w,q,wd,m_name,wd_name)
SELECT @date := date_add(@date, interval 1 day) as date,
    # integer ID that allowsimmediate understanding
    date_format(@date, "%Y%m%d")as id,
    case when @y<>year(@date) and @report_w<>0 then @bz:=1
         when @report_w<>0 and @bz=1 then @bz:=1
         else @bz:=0 end bz,
    case when @bz=1 and weekday(@date)=3 then @report_w:=0
         when @y=year(@date) and weekday(@date)=3 then @report_w:=@report_w+1
         when @y<>year(@date) and weekday(@date)=3 then @report_w:=0 else @report_w end report_w,
    @y:=year(@date) as y,
    month(@date) as m,
    day(@date) as d,
    date_format(@date, "%x")as yw,
    week(@date, 3) as w,
    quarter(@date) as q,
    weekday(@date)+1 as wd,
    monthname(@date) as m_name,
    dayname(@date) as wd_name
FROM t_dim_article_youhui
WHERE date_add(@date, interval 1 day) <= @d1
ORDER BY date;
########################################################
# time span
SET @d0 = "2012-01-01";
SET @d1 = "2019-01-01";
SET @date = date_sub(@d0, interval 1 day);
set @report_w=0;
set @ht_report_w=0;
# set up the time dimension table
DROP TABLE IF EXISTS dim_time;
CREATE TABLE `dim_time` (
  `date` date DEFAULT NULL,
  `id` int NOT NULL,
  `y` smallint DEFAULT NULL,
  `m` smallint DEFAULT NULL,
  `d` smallint DEFAULT NULL,
  `yw` smallint DEFAULT NULL,
  `w` smallint DEFAULT NULL,
  `q` smallint DEFAULT NULL,
  `wd` smallint DEFAULT NULL,
  `m_name`  char(10) DEFAULT NULL,
  `wd_name` char(10) DEFAULT NULL,
  `report_w` int(10) DEFAULT NULL,
  `ht_report_w` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`));
# populate the table with dates
INSERT INTO dim_time(date,id,report_w,ht_report_w,y,m,d,yw,w,q,wd,m_name,wd_name)
SELECT @date := date_add(@date, interval 1 day) as date,
    # integer ID that allowsimmediate understanding
    date_format(@date, "%Y%m%d")as id,
    case when weekday(@date)=3 then @report_w:=@report_w+1 else @report_w end report_w,
    case when weekday(@date)=4 then @ht_report_w:=@ht_report_w+1 else @ht_report_w end ht_report_w,
    year(@date) as y,
    month(@date) as m,
    day(@date) as d,
    date_format(@date, "%x")as yw,
    week(@date, 3) as w,
    quarter(@date) as q,
    weekday(@date)+1 as wd,
    monthname(@date) as m_name,
    dayname(@date) as wd_name
FROM t_dim_article_youhui
WHERE date_add(@date, interval 1 day) <= @d1
ORDER BY date;
