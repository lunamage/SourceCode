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
