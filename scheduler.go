// Copyright 2014 JD Inc. All Rights Reserved.
// Author: zhangwei
// email : zhangwei_2943@163.com
// date  : 2014-12-19
//==============================================================
// JAE调度中心,负责收集所有资源池的数据,根据用户需求从资源池中
// 获取一个最优的资源
//==============================================================

package main 

import (

 "github.com/cloudfoundry/yagnats"
 steno "github.com/cloudfoundry/gosteno"
 "scheduler/codec"
 "scheduler/config"
 "scheduler/deapool"
 "scheduler/droplet"
 "scheduler/controller"
 "flag"
 "strings"
 "fmt"
 "time"
 "scheduler/apppackage"
 "scheduler/buildpackcache"
 "scheduler/listener"
)

var configFile string

func init(){
	flag.StringVar(&configFile, "c", "", "Configuration File")
	
	flag.Parse()
}

func main() {
	
	var err error
	
	//初始化配置文件
	c := config.DefaultConfig()
	
	if configFile != "" {
	
		c = config.InitConfigFromFile(configFile)
	}
	
	//初始化logger
	
	InitLoggerFromConfig(c)
	logger := steno.NewLogger("cc_helper")
	
	logger.Info("logger config: file:"+c.Logging.File+", level:"+c.Logging.Level+"")
	
	//初始化nats 客户端
	logger.Info("logger config: Host:"+c.Nats.Host+",User:"+c.Nats.User+",pass:"+c.Nats.Pass+"")
	natsClient := yagnats.NewClient()
	addr := c.Nats.Host
	
	if !strings.HasPrefix(addr, "zk://") {
		addr = fmt.Sprintf("%s:%d", c.Nats.Host, c.Nats.Port)
	}
	
	natsInfo := &yagnats.ConnectionInfo{
		Addr:     addr,
		Username: c.Nats.User,
		Password: c.Nats.Pass,
	}
	
	err = natsClient.Connect(natsInfo)
	
	for ; err != nil; {
		err = natsClient.Connect(natsInfo)
		logger.Errorf("Could not connect to NATS: ", err.Error())
		time.Sleep(500 * time.Millisecond)
	}
	
	logger.Infof("helper config:%v",c)
	Run(c, natsClient)
}

//初始化日志信息
func InitLoggerFromConfig(c *config.Config){

	l, err := steno.GetLogLevel(c.Logging.Level)
	if err != nil {
		panic(err)
	}

	s := make([]steno.Sink, 0, 3)
	s = append(s, steno.NewFileSink(c.Logging.File))

	stenoConfig := &steno.Config{
		Sinks: s,
		Codec: codec.NewStringCodec(),
		Level: l,
	}

	steno.Init(stenoConfig)
}

//启动实例
func Run(c *config.Config, mbus yagnats.NATSClient){
	deaPool := deapool.NewPool(c, mbus)
	droplet := droplet.NewDropletMgm(c)
	packages := apppackage.NewPackageMgm(c)
	buildpack := buildpackcache.NewBuildpackMgm(c)
	listener := listener.NewListener(c, mbus, droplet, packages, buildpack)
	
	controller := controller.NewController(c, deaPool, droplet, packages, buildpack)
	
	deaPool.Start()
	listener.Start()
	controller.Start()
}
