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


docker run hello-world

#
docker run --name mysql-test -e MYSQL_ROOT_PASSWORD=root -d mysql
docker exec -it mysql-test bash
mysql -h localhost -u root -p'root'

docker ps
docker stop 55df3409137e

docker container ls

docker ps -a


apt-get update
apt-get  install vim*


docker login
qianbenying9
123578951
