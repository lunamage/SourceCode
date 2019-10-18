SET @curRank := 0;
SET @date:= NULL;

SELECT IF(@date = date1, @curRank := @curRank + 1, @curRank := 1) AS rank,
       @date,
       @date := date1,
       totart,
       mall
  FROM (SELECT DATE_FORMAT(pubdate, '%Y-%m') date1,
               COUNT(id) totart,
               domain_name_top mall
          FROM t_dim_article_youhui
         WHERE     channel = 5
               AND DATE_FORMAT(pubdate, '%Y-%m') IN ('2014-01', '2014-02')
        GROUP BY domain_name_top, DATE_FORMAT(pubdate, '%Y-%m')
        ORDER BY date1 ASC, totart DESC) F;
