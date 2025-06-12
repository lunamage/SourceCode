cache table tmp as
select '点击' t1,'001' user_id,'00:00:01' time,'电脑数码' cate1
union all
select '曝光' t1,'001' user_id,'00:00:02' time,'' cate1
union all
select '曝光' t1,'001' user_id,'00:00:03' time,'' cate1
union all
select '点击' t1,'001' user_id,'00:00:04' time,'日用百货' cate1
union all
select '曝光' t1,'001' user_id,'00:00:05' time,'' cate1;

select t1,user_id,time,
if(rank<>0,first_value(cate1)over(partition by user_id,rank order by time),'') cate1_new
from(
select sum(if(t1='点击',1,0))over(partition by user_id order by time) rank,
t1,user_id,time,cate1
from tmp) a;


–conf spark.executor.extraJavaOptions="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8"

String.format("ib_%s_%s", "123456789", 3)
ib_123456789_3
