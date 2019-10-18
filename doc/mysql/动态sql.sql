mysql> PREPARE stmt1 FROM 'SELECT SQRT(POW(?,2) + POW(?,2)) AS hypotenuse';
mysql> SET @a = 3;
mysql> SET @b = 4;
mysql> EXECUTE stmt1 USING @a, @b;
--------------------------------------------------------------------------------

set @a= 'select * from t_dim_article_youhui limit 10';
PREPARE stmt1 FROM @a;
EXECUTE stmt1;
