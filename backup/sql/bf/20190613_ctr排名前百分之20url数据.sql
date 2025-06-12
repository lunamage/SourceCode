create table bi_test.zyl_tmp_190610 as
select id,PERCENT_RANK()over(partition by dt order by sl2/sl1 desc) rank,dt
from(
select dt,dim3 id,
sum(if(metric='article_exposure_count',`values` ,0)) sl1,
sum(if(metric='article_rec_details_click',`values` ,0)) sl2
from bi_app_ga.app_ga_recommend_statistics_sdk
where dt between '2018-10-01' and '2019-05-31'
and metric in('article_exposure_count','article_rec_details_click')
group by dt,dim3) a
where sl1>5000;

create table bi_test.zyl_tmp_190610_1 as
select b.clean_link,min(if(b.digital_price=0,1000000,b.digital_price)) digital_price
from(
select distinct id
from bi_test.zyl_tmp_190610
where rank<=0.05 and dt between '2018-10-01' and '2019-04-30' ) a
inner join (select id,clean_link,cast(digital_price as double) digital_price  from stg.db_youhui_youhui where dt='2019-06-09' and pubdate<='2019-04-30 23:59:59') b on a.id=b.id
group by b.clean_link;


create table bi_test.zyl_tmp_190610_2 as
select a.dt,a.rank,b.clean_link,b.digital_price
from(
select distinct dt,id,rank
from bi_test.zyl_tmp_190610
where dt between '2019-05-01' and '2019-05-31' ) a
inner join (select id,clean_link,cast(digital_price as double) digital_price,to_date(pubdate) pubdate from stg.db_youhui_youhui where dt='2019-06-09') b on a.id=b.id and a.dt=b.pubdate;


select a.dt,
count(*) sl,
sum(if(a.digital_price<=b.digital_price,1,0)) sl2,
sum(if(a.digital_price<=b.digital_price and a.rank<=0.2,1,0)) sl3,
sum(if(a.digital_price<=b.digital_price,1,0))/count(*) bl1,
sum(if(a.digital_price<=b.digital_price and a.rank<=0.2,1,0))/sum(if(a.digital_price<=b.digital_price,1,0)) bl2
from bi_test.zyl_tmp_190610_2 a
left join bi_test.zyl_tmp_190610_1 b on a.clean_link=b.clean_link
group by a.dt order by dt;



create table bi_test.zyl_tmp_190610_3 as
select b.clean_link,min(if(b.digital_price=0,1000000,b.digital_price)) digital_price
from(
select distinct id
from bi_test.zyl_tmp_190610
where rank<=0.15 and dt between '2018-10-01' and '2019-04-30' ) a
inner join (select id,clean_link,cast(digital_price as double) digital_price  from stg.db_youhui_youhui where dt='2019-06-09' and pubdate<='2019-04-30 23:59:59') b on a.id=b.id
group by b.clean_link;


select a.dt,
count(*) sl,
sum(if(a.digital_price<=b.digital_price,1,0)) sl2,
sum(if(a.digital_price<=b.digital_price and a.rank<=0.2,1,0)) sl3,
sum(if(a.digital_price<=b.digital_price,1,0))/count(*) bl1,
sum(if(a.digital_price<=b.digital_price and a.rank<=0.15,1,0))/sum(if(a.digital_price<=b.digital_price,1,0)) bl2
from bi_test.zyl_tmp_190610_2 a
left join bi_test.zyl_tmp_190610_3 b on a.clean_link=b.clean_link
group by a.dt order by dt;
