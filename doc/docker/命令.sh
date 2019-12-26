#启动
docker-machine ssh default
#重启
docker-machine restart default
#命令停止default虚拟机
docker-machine stop default

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
docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 mysql:5.6
docker exec -it mysql-test bash
mysql -h localhost -u root -p'root'
#redis
docker run -p 6379:6379 -d redis redis-server
docker exec -it 15c580b282b3 redis-cli -h localhost -p 6379


###########################################################################
apt-get update
apt-get install vim*


docker login
qianbenying9
123578951
