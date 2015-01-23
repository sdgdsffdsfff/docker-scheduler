package listener

import (
	"scheduler/droplet"
	"scheduler/apppackage"
	"scheduler/buildpackcache"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/yagnats"
	"time"
	"scheduler/config"
	"scheduler/util"
	"encoding/json"
	"fmt"
	"sync"
)

//	1.负责监控本地缓存磁盘使用情况
//	2.负责清理本地缓存
//	3.负责删除jss中的数据(比如应用删除时 需要删除对应的droplet 和 package)
type Listener struct {
	lock 				sync.Mutex
   droplet				*droplet.DropletMgm
   packages				*apppackage.PackageMgm
   buildpack			*buildpackcache.BuildpackMgm
   messageBus     		yagnats.NATSClient
   logger         		*steno.Logger
   dropletJss			*util.JssUtil
   packageJss    		*util.JssUtil
   ticker 		   		*time.Ticker
   timeOutThreshold 	time.Duration
   CacheTimeOut			int
}

//应用打包成功需要监听消息,删除本地缓存和 jss
type StagingSucc struct {
	Guid string `json:"guid"`
}

//新建listener 对象
func NewListener(c *config.Config, mbus yagnats.NATSClient, dropletMgm *droplet.DropletMgm, packageMgm *apppackage.PackageMgm, buildpackmgm *buildpackcache.BuildpackMgm) *Listener{
	
	return &Listener{
		droplet:		dropletMgm,
		packages:		packageMgm,
		buildpack:		buildpackmgm,
		messageBus:		mbus,
		logger: 		steno.NewLogger("cc_helper"),
		dropletJss:		util.NewJssUtil(c, true),
		packageJss:		util.NewJssUtil(c, false),
		timeOutThreshold: time.Duration(c.Droplet.CacheInterval) * time.Second,
		CacheTimeOut: c.Droplet.CacheTimeOut,
	}
}

//启动listener
func (l *Listener) Start(){
	l.subScribeDelApp()
	l.subScribeStagingSuccess()
	l.startPruningCycle()
}

//监控本地缓存使用情况
func (l *Listener) startPruningCycle (){
	if l.CacheTimeOut >0 { //超时时间秒
		l.lock.Lock()
		l.ticker = time.NewTicker(l.timeOutThreshold)
		l.lock.Unlock()
		
		go func() {
			for {
				select {
				case <-l.ticker.C:
					l.droplet.CheckCacheDroplet()
					l.buildpack.CheckCacheBuildpack()
				}
			}
		}()
	}
}

//监听应用打包成功消息
func (l *Listener) subScribeStagingSuccess() {
	l.logger.Infof("start sub-scribe topic: jae.staging.success")
	l.messageBus.Subscribe("jae.staging.success", func(message *yagnats.Message) {
	
		start := time.Now()
		payload := message.Payload
		var msg StagingSucc

		err := json.Unmarshal(payload, &msg)
		
		if err != nil {
			logMessage := fmt.Sprintf("%s: Error unmarshalling JSON (%d; %s): %s", "jae.staging.success", len(payload), payload, err)
			l.logger.Warnd(map[string]interface{}{"payload": string(payload)}, logMessage)
		}
		
		guid := msg.Guid
		l.logger.Infof("process jae.staging.success app guid:%v",guid)
		
		if guid == "" {
			return
		}
		
		//删除本地缓存文件
		l.packages.DestoryPackage(guid)
		l.droplet.DestoryDroplet(guid)
		
		//删除jss
		l.packageJss.Remove(guid)
		
		end := time.Now()
		l.logger.Infof("应用打包成功 清空jss,cache(app packages) 耗时:%v",end.Sub(start))
	})
}

//监听删除应用的消息,删除应用需要同时删除 jss,本地缓存的 droplet / app package
func (l *Listener) subScribeDelApp(){

	l.logger.Infof("start sub-scribe topic: jae.deleted")
	l.messageBus.Subscribe("jae.deleted", func(message *yagnats.Message) {
	
		start := time.Now()
		guid := string(message.Payload)
		l.logger.Infof("process jae delete app guid:%v",guid)
		
		if guid == "" {
			return
		}
		
		//删除本地缓存文件
		l.packages.DestoryPackage(guid)
		l.droplet.DestoryDroplet(guid)
		l.buildpack.DestoryBuildpack(guid)
		
		//删除jss
		l.dropletJss.Remove(guid)
		l.packageJss.Remove(guid)
		
		end := time.Now()
		l.logger.Infof("删除应用app 清空jss,cache 耗时:%v",end.Sub(start))
	})
}
