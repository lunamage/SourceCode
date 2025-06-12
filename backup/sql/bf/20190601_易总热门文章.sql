select article_id,pubdate,channel,cate1,logtype,small,mall,type,from_base64(title) as title,
sum(click_count)/sum(exposure_count) as CTR,
sum(event_count)/sum(click_count) as LTR, concat("https://www.smzdm.com/p/",article_id,"/") as url,
sum(exposure_count) exposure_count
from app_recommend_issue_article
where exposure_count>=1000 and pubdate>DATE_SUB(NOW(), INTERVAL 1 day)
group by article_id,pubdate,channel,cate1,logtype,small,mall,type,from_base64(title),concat("https://www.smzdm.com/p/",article_id,"/")
order by CTR  desc limit 0,10;
