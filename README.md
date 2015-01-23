scheduler
====
docker 中央调度系统,负责所有vm机器的调度,从中寻找到最合适的vm进行任务处理


==============
install
==============

1. 下载go语言包
	download go1.2.linux-adm64.tar.gz
	
2. 配置go环境变量
	export GIT_SSL_NO_VERIFY=1
	export GOROOT=/export/service/go
	export GOPATH=/export/service/gopath
	export GOBIN=$GOROOT/bin
	export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
	
3.提前下载依赖包到 $GOPATH/src
	下载 yagnats 到 $GOPATH/src/github.com/cloudfoundry/
	
4.下载 builder
	cd $GOPATH/src && git clone http://icode.jd.com/cdlxyong/scheduler.git

5:编译
	cd $GOPATH/src/scheduler/ && go get -v ./...
	
6:修改配置文件
	默认配置在 $GOPATH/src/scheduler/config.yml
	
7:启动
	scheduler -c config_path
	or 
	nohup scheduler -c config.yml > /export/home/jae/scheduler.out
