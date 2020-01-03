select * from(
select cd11,cd20,cd30,a.el,
row_number()over(partition by cd11 order by rand()) r
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'11','20','30') b as cd11,cd20,cd30
where a.dt='2020-01-02' and a.ec='详情页' and a.ea='详情页阅读' and cd20 in('3','76')) a
where r<3 order by cd20;
----

select a.dt,count(distinct a.id,a.uid) sl
from(
select a.dt,b.a id,a.uid
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-01-01' and '2020-01-02'
and a.ec='01' and a.ea='01' and b.sp='0') a
inner join (select id from bi_dw_ga.dim_article_info where channel_id in('1','2','5') and mall in('聚划算','天猫精选','天猫超市','天猫国际','飞猪','95095医药','天猫电器城','天猫国际官方直营','淘宝心选')) b on a.id=b.id
group by a.dt;


2019-01-01	38579662
2019-01-02  45531684
2020-01-01  50535983
2020-01-02	70471813
-----
flink run -q -m yarn-cluster -c recommend.artRead.ArtReadFeatureNew -yqu bitmp -ynm ArtReadFeatureNew -p 4 -yn 2 -ys 2 -ytm 24480 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-test.jar &

redis-cli -h 10.42.168.37 -p 6379
