--为了验证用户交互数据和社区留存的关系，现申请导出交互数据表
--
--【数据】
--用户id
--用户注册时间
--当天对所有文章的“顶”数
--当天对所有文章的“评论”数
--当天对所有文章的“收藏”数
--当天对所有文章的“打赏”次数
--当天对所有文章的“分享”次数
--该用户次日留存（1:留存/0:非留存）
--该用户2日留存（1/0）
--该用户3日留存（1/0）
--该用户4日留存（1/0）
--该用户5日留存（1/0）
--该用户6日留存（1/0）
--该用户7日留存（1/0）
--日期（dt）
--
--【日期范围】
--2020-06-22 ～ 2020-07-22
create table bi_test.zyl_tmp_200723_1 as
select distinct a.dt,a.uid user_id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','pid') b as a,c,tv,p,pid
where a.dt between '2020-06-22' and '2020-07-22'
and a.ec='09' and a.ea='01'
and a.uid<>'' and a.uid<>'0'
and b.c in('11','31','66','6','8');


create table bi_test.zyl_tmp_200723_2 as
select a.dt,b.smzdm_id,b.user_register_time,
sum(rating) rating,
sum(comment) comment,
sum(favorites) favorites,
sum(shang) shang,
sum(share) share
from(
select dt,user_id,count(*) rating,0 comment,0 favorites,0 shang,0 share
from stg.db_userlog_article_rating_log
where rating=1
and dt between '2020-06-22' and '2020-07-22'
and channel_id in('6','8','11','31','66','76')
group by dt,user_id
union all
select to_date(creation_date) dt,user_id,0,count(*) comment,0,0,0
from bi_dw.fact_comment_base
where to_date(creation_date) between '2020-06-22' and '2020-07-22'
and channel_id in('6','8','11','31','66','76')
group by to_date(creation_date),user_id
union all
select dt,user_id,0,0,count(*) favorites,0,0
from stg.db_userlog_article_favorites_log
where action=1
and dt between '2020-06-22' and '2020-07-22'
and channel_id in('6','8','11','31','66','76')
group by dt,user_id
union all
select dt,user_id,0,0,0,count(*) shang,0
from stg.db_userlog_shang_log
where dt between '2020-06-22' and '2020-07-22'
and channel_id in('6','8','11','31','66','76')
group by dt,user_id
union all
select dt,user_id,0,0,0,0,count(*) share
from stg.db_userlog_user_share_log
where dt between '2020-06-22' and '2020-07-22'
and channel_id in('6','8','11','31','66','76')
group by dt,user_id) a
inner join(select user_id,smzdm_id,creation_date user_register_time from stg.db_udc_account where dt='2020-07-22') b on a.user_id=b.user_id
group by a.dt,b.smzdm_id,b.user_register_time;

create table bi_test.zyl_tmp_ai237 as
select a.smzdm_id,
a.user_register_time,
a.rating,
a.comment,
a.favorites,
a.shang,
a.share,
if(b1.user_id is null,0,1) retention_1,
if(b2.user_id is null,0,1) retention_2,
if(b3.user_id is null,0,1) retention_3,
if(b4.user_id is null,0,1) retention_4,
if(b5.user_id is null,0,1) retention_5,
if(b6.user_id is null,0,1) retention_6,
if(b7.user_id is null,0,1) retention_7
from bi_test.zyl_tmp_200723_2 a
left join bi_test.zyl_tmp_200723_1 b1 on a.smzdm_id=b1.user_id and a.dt=date_add(b1.dt,1)
left join bi_test.zyl_tmp_200723_1 b2 on a.smzdm_id=b2.user_id and a.dt=date_add(b2.dt,2)
left join bi_test.zyl_tmp_200723_1 b3 on a.smzdm_id=b3.user_id and a.dt=date_add(b3.dt,3)
left join bi_test.zyl_tmp_200723_1 b4 on a.smzdm_id=b4.user_id and a.dt=date_add(b4.dt,4)
left join bi_test.zyl_tmp_200723_1 b5 on a.smzdm_id=b5.user_id and a.dt=date_add(b5.dt,5)
left join bi_test.zyl_tmp_200723_1 b6 on a.smzdm_id=b6.user_id and a.dt=date_add(b6.dt,6)
left join bi_test.zyl_tmp_200723_1 b7 on a.smzdm_id=b7.user_id and a.dt=date_add(b7.dt,7);
--spark2-sql --jars /data/source/data_warehouse/pub_jar/mysql-connector-java-commercial-5.1.40-bin.jar --driver-class-path /data/source/data_warehouse/pub_jar/mysql-connector-java-commercial-5.1.40-bin.jar --name 'zyl_test' --queue bi --driver-memory 4g --executor-cores 6 --master yarn --executor-memory 20g --num-executors 20 --conf spark.default.parallelism=800 --conf spark.sql.shuffle.partitions=800 --conf spark.scheduler.listenerbus.eventqueue.capacity=100000
