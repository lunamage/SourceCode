#1,查询系统表中的锁信息
select * from information_schema.INNODB_LOCK_WAITS;
select * from information_schema.INNODB_LOCKS;
select * from information_schema.INNODB_TRX;
#发生死锁时查询死锁发生的SQL和时间
show engine innodb status;
#显示当前查询
show full processlist;
#查询innodb的锁等待超时配置
show VARIABLES like '%innodb_lock_wait_timeout%';
