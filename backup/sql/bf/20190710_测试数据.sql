insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005852311'),70058523,'11',10511);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005865411'),70058654,'11',30000);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7004620611'),70046206,'11',36500);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005848311'),70058483,'11',1000000);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005846111'),70058461,'11',2000000);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7004715811'),70047158,'11',9999);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005859311'),70058593,'11',9000);



PV=1万，文章id：70058625
PV=10511  文章id：70058634
pv=3万    文章id：70058620
pv=36500  文章id：70058594
pv=100万   文章id：70058287
pv=200万   文章id：70058626
pv=9999    文章id：70058563
pv=9000    文章id：70058585

select *
from t_bi_cms_article_pv
where article_id in('70058625','70058634','70058620','70058594','70058287','70058626','70058563','70058585');


insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005862511'),70058625,'11',10000);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005863411'),70058634,'11',10511);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005862011'),70058620,'11',30000);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005859411'),70058594,'11',36500);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005828711'),70058287,'11',1000000);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005862611'),70058626,'11',2000000);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005856311'),70058563,'11',9999);
insert into t_bi_cms_article_pv(id,article_id,channel_id,pv) values(md5('7005858511'),70058585,'11',9000);
