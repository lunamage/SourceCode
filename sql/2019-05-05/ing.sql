insert into app_rec_realtime_bak(stat_dt,abtest,exposure,click,event)
select stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between DATE_SUB(curdate(),INTERVAL 1 DAY) and concat(DATE_SUB(curdate(),INTERVAL 1 DAY),' 23:59:59')
group by stat_dt,abtest;



--总表
select stat_dt,abtest,exposure,click,event,click/exposure ctr,event/click bl,event/exposure bl2
from(
select date_format(stat_dt,'%Y-%m-%d') stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between CURRENT_DATE() and concat(CURRENT_DATE(),' 23:59:59')
and abtest not in ('无','smzdm','')
group by date_format(stat_dt,'%Y-%m-%d'),abtest) a
order by bl2;

--具体流量明细表
select stat_dt,abtest,exposure,click,event,click/exposure ctr,event/click bl
from(
select stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between CURRENT_DATE() and concat(CURRENT_DATE(),' 23:59:59') and abtest='a'
group by stat_dt,abtest) a
order by stat_dt;

select count(*),max(load_date) from app.app_rec_realtime;

select date_format(stat_dt,'%Y-%m-%d') d,count(*) from app.app_rec_realtime group by date_format(stat_dt,'%Y-%m-%d');

select avg(sl)
from(
select date_format(load_date,'%Y-%m-%d %H:%i'),count(*) sl
from app.app_rec_realtime group by date_format(load_date,'%Y-%m-%d %H:%i')) a;

select * from app.app_rec_realtime  order by load_date desc limit 100;
