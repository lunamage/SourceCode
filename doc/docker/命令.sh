#重启
docker-machine restart default
#命令停止default虚拟机
docker-machine stop default
#启动
docker-machine ssh default
#查看设备
docker-machine ls

#default虚拟机的默认用户名和密码
用户名：docker
密码： tcuser


#阿里加速器 https://cr.console.aliyun.com/cn-hangzhou/instances/mirrors
sudo sed -i "s|EXTRA_ARGS='|EXTRA_ARGS='--registry-mirror=https://36s3wvt2.mirror.aliyuncs.com |g" /var/lib/boot2docker/profile

#查看所有容器
docker ps -a

#mysql
docker cp 0bf03e4db5e0:/etc/mysql  /Users/smzdm/dockerConfig/etc_mysql
docker cp 0bf03e4db5e0:/var/lib/mysql  /Users/smzdm/dockerConfig/var_lib_mysql

docker run -d --name mysql --network testnet -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 -v /Users/smzdm/dockerConfig/etc_mysql:/etc/mysql  -v /Users/smzdm/dockerConfig/var_lib_mysql:/var/lib/mysql mysql:5.6
docker exec -it mysql bash
mysql -h localhost -u root -p'123456'
show variables like '%log_bin%';
docker restart mysql




docker run --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.6
docker exec mysql bash -c "echo 'log-bin=/var/lib/mysql/mysql-bin' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql bash -c "echo 'server-id=123454' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker exec mysql bash -c "echo 'binlog-format=ROW' >> /etc/mysql/mysql.conf.d/mysqld.cnf"
docker restart mysql

#redis
docker run -p 6379:6379 -d redis redis-server
docker exec -it 15c580b282b3 redis-cli -h localhost -p 6379
#maxwell
docker pull zendesk/maxwell
# 启动maxwell，并将解析出的binlog输出到控制台
docker run -ti --network testnet --rm zendesk/maxwell:v1.19.7 bin/maxwell --user='root' --password='123456' --host='mysql' --producer=stdout

docker run -ti --network testnet --rm zendesk/maxwell:v1.19.7 bin/maxwell-bootstrap --user='root' --password='123456' --host='mysql'  --database='app' --table t_conf_event_at_mall --client_id maxwell

docker run -it --name maxwell -d zendesk/maxwell:v1.19.7
docker exec -it maxwell bash
docker restart maxwell
##
docker run -it --name ververica -d fintechstudios/ververica-platform-k8s-operator

###############
docker network create testnet

--network testnet

###########################################################################
apt-get update
apt-get install vim*


docker login
qianbenying9
123578951
