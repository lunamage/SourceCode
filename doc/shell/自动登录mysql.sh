--cat ssh_auto_login.sh
#!/usr/bin/expect
##
# ssh模拟登陆器
##
set dbname [lindex $argv 0]
set dbuser [lindex $argv 1]
set dbpassword [lindex $argv 2]
set dbport [lindex $argv 3]
set db [lindex $argv 4]
set timeout 10
spawn mysql -h $dbname -P$dbport -u$dbuser -p
exec sleep 1
expect "*" {send "$dbpassword\r";}
exec sleep 1
expect "*" {send "use $db\r";}
expect "*" {send "show tables\;\r";}
interact

--------------------------------------------------------------------------------
--cat login.sh
#!/bin/sh
##
# 服务器登陆器
##
if [ $# -lt 1 ];then
 echo 'dw;zhongce;baoliao;merchant;youhui;yuanchuang;smzdm;mall;userlog;comment;shai;gmv;gmvtemp;wiki;user;'
fi
case "$1" in
 dw)
  expect /root/ssh_auto_login.sh dev_ga_data_warehouse_mysql_m01 wdDBUser "2Gb(tv+-n" 3306 dev_ga_data_warehouse;;
 zhongce)
  expect /root/ssh_auto_login.sh zhongce_db_mysql_m01 smzdm "sMzdmTest" 3306 smzdm_probation;;
 baoliao)
  expect /root/ssh_auto_login.sh baoliao_db_mysql_m01 baoliao_user "X2oZadMHC" 3401 dbzdm_baoliao;;
 merchant)
  expect /root/ssh_auto_login.sh shangjia_db_mysql_m01 merchant_user "LFBxA45WSS" 3306 MerchantDB;;
 youhui)
  expect /root/ssh_auto_login.sh youhui_db_mysql_s11 youhui_user "xlJ7Enmhs" 3403 dbzdm_youhui;;
 yuanchuang)
  expect /root/ssh_auto_login.sh yuanchuang_db_mysql smzdm_post "smzdmPost_162304" 3306 smzdm_yuanchuang;;
 smzdm)
  expect /root/ssh_auto_login.sh smzdm_mysql_s11 smzdm "sMzdmTest" 3306 smzdm;;
 mall)
  expect /root/ssh_auto_login.sh smzdm_dbzdm_link link_user "X2oZadMHC" 3400 dbzdm_link;;
 userlog)
  expect /root/ssh_auto_login.sh smzdm_userLogdb_mysql_m01 smzdm_users "Users_162304" 3404 UserLogDB;;
 comment)
  expect /root/ssh_auto_login.sh dev_smzdm_base_CommonadminDB smzdmdb "smzdmDB_162304" 3306 CommentAdminDB;;
 shai)
  expect /root/ssh_auto_login.sh smzdm_dbzdm_shai shaiUser "qwRSriguAzT" 3306 shaiDB;;
 gmv)
  expect /root/ssh_auto_login.sh smzdm_gmv zdmgmvUser "mGtX9dAd-GMIQG" 3306 zdmgmvDB;;
 gmvtemp)
  expect /root/ssh_auto_login.sh smzdm_middle_gmv gmvUser "nBj60LYN7MlACXRl" 3306 gmvTempDB;;
 dingyue)
  expect /root/ssh_auto_login.sh dingyuedb_mysql_s01 smzdm_dy "smzdmdy_162304" 3402 smzdm_dingyue;;
 wiki)
  expect /root/ssh_auto_login.sh product_db_mysql_m01 productAdmin "productTest" 3306 product;;
 user)
  expect /root/ssh_auto_login.sh smzdm_userdb_mysql_m01 smzdm_users "Users_162304" 3404 UserDB;;
esac
--------------------------------------------------------------------------------
--新
#!/usr/bin/expect
##
# ssh模拟登陆器
##
set dbname [lindex $argv 0]
set dbuser [lindex $argv 1]
set dbpassword [lindex $argv 2]
set dbport [lindex $argv 3]
set db [lindex $argv 4]
set timeout 10
spawn mysql -h $dbname -P$dbport -u$dbuser -p
exec sleep 1
expect "*" {send "$dbpassword\r";}
exec sleep 1
expect "*" {send "use $db\r";}
expect "*" {send "show tables\;\r";}
interact
--------------------------------------------------------------------------------
--新
#!/bin/sh
##
# 服务器登陆器
##
if [ $# -lt 1 ];then
 echo 'dw;zhongce;baoliao;merchant;youhui;yuanchuang;smzdm;mall;userlog;comment;shai;gmv;gmvtemp;wiki;user;jinbi;youhuiquan;dingyue;xianzhi;'
fi
case "$1" in
 dw)
  expect /root/ssh_auto_login.sh dev_ga_data_warehouse_mysql_m01 wdDBUser "2Gb(tv+-n" 3306 dev_ga_data_warehouse;;
 zhongce)
  expect /root/ssh_auto_login.sh zhongce_db_mysql_m01 smzdm "sMzdmTest" 3306 smzdm_probation;;
 baoliao)
  expect /root/ssh_auto_login.sh baoliao_db_mysql_m01 baoliao_user "X2oZadMHC" 3401 dbzdm_baoliao;;
 merchant)
  expect /root/ssh_auto_login.sh shangjia_db_mysql_m01 merchant_user "LFBxA45WSS" 3306 MerchantDB;;
 youhui)
  expect /root/ssh_auto_login.sh youhui_db_mysql_s11 youhui_user "xlJ7Enmhs" 3403 dbzdm_youhui;;
 yuanchuang)
  expect /root/ssh_auto_login.sh yuanchuang_db_mysql smzdm_post "smzdmPost_162304" 3306 smzdm_yuanchuang;;
 smzdm)
  expect /root/ssh_auto_login.sh smzdm_mysql_s11 smzdm "sMzdmTest" 3306 smzdm;;
 mall)
  expect /root/ssh_auto_login.sh smzdm_dbzdm_link link_user "X2oZadMHC" 3400 dbzdm_link;;
 userlog)
  expect /root/ssh_auto_login.sh smzdm_userLogdb_mysql_m01 smzdm_users "Users_162304" 3404 UserLogDB;;
 comment)
  expect /root/ssh_auto_login.sh dev_smzdm_base_CommonadminDB smzdmdb "smzdmDB_162304" 3306 CommentAdminDB;;
 shai)
  expect /root/ssh_auto_login.sh smzdm_dbzdm_shai shaiUser "qwRSriguAzT" 3306 shaiDB;;
 gmv)
  expect /root/ssh_auto_login.sh smzdm_gmv zdmgmvUser "mGtX9dAd-GMIQG" 3306 zdmgmvDB;;
 gmvtemp)
  expect /root/ssh_auto_login.sh smzdm_middle_gmv gmvUser "nBj60LYN7MlACXRl" 3306 gmvTempDB;;
 dingyue)
  expect /root/ssh_auto_login.sh dingyuedb_mysql_s01 smzdm_dy "smzdmdy_162304" 3402 smzdm_dingyue;;
 wiki)
  expect /root/ssh_auto_login.sh product_db_mysql_m01 productAdmin "productTest" 3306 product;;
 user)
  expect /root/ssh_auto_login.sh smzdm_userdb_mysql_m01 smzdm_users "Users_162304" 3404 UserDB;;
 jinbi)
  expect /root/ssh_auto_login.sh smzdm_dbsmzdm_jinbi smzdm_users "Users_162304" 3404 PointDB;;
 youhuiquan)
  expect /root/ssh_auto_login.sh smzdm_dbzdm_exchangedb exchange_user "jueyNGhD4CpM" 3406 ExchangeDB;;
 dingyue)
  expect /root/ssh_auto_login.sh dingyuedb_mysql_s01 smzdm_dy "smzdmdy_162304" 3402 smzdm_dingyue;;
 xianzhi)
  expect /root/ssh_auto_login.sh smzdm_secondDB_mysql_m01 SecondDB_user "P7gJFQomCl6H" 3405 SecondDB;;
 yangben)
  expect /root/ssh_auto_login.sh blSample blSample "nnIEN34aIP65" 3306 dbzdm_blSample;;
esac
