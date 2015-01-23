package deapool

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/yagnats"
	"scheduler/config"
	"sort"
	"math/rand"
)

type DeaPool struct{
	
	timeOutSecond		int
	timeOutThreshold  	time.Duration
	messageBus        	yagnats.NATSClient
	logger            	*steno.Logger
	lock 				sync.Mutex
	endpoints        	map[string]*DeaAdvertisement
	ticker 		   		*time.Ticker
}

func NewPool(c *config.Config, mbus yagnats.NATSClient) *DeaPool{

	return &DeaPool{
		timeOutSecond: 						c.DeaTimeoutSecond,
		timeOutThreshold:                   time.Duration(c.DeaTimeoutSecond) * time.Second,
		messageBus:							mbus,
		logger: 							steno.NewLogger("cc_helper"),
		endpoints:  						make(map[string]*DeaAdvertisement),
	}
	
}

//dea池中每个item的消息
type poolItemMessage struct{
	Id 						string 				`json:"id"`
	Stacks    				[]string      		`json:"stacks"`
	Available_memory		int					`json:"available_memory"`
	Available_disk			int 				`json:"available_disk"`
	AppIdToCount			map[string]int 		`json:"app_id_to_count"`
	PlacementProperties 	map[string]string   `json:"placement_properties"`
	DockerVm				bool 				`json:"docker_vm"`
}

//dea shutdown message
type poolItemDownMessage struct {
	Id 						string	      		`json:"id"`
	Ip						string 				`json:"ip"`
	Version					string				`json:version`
	AppIdToCount			map[string]int 		`json:"app_id_to_count"`
}

//查找dea的message
type FindDeaMessage struct{
	AppId 				   string            	`json:"appId"`
	Memory				   int					`json:"memory"`
	Disk				   int               	`json:"disk"`
	Stacks             	   string             	`json:"stacks"`
	//true:只返回当前应用运行在的 deaID 集合
	OwnerApp			   bool					`json:"ownerApp"`
	//true:返回除开运行当前应用的所有dea，从其他dea中选择一个资源最优的一个
	OtherDea			   bool 				`json:"otherDea"`
	Docker				   bool					`json:"docker"`
}


//每个dea资源对象信息
type DeaAdvertisement struct{
	Id   					 	string
	Stacks                 		[]string
	ByStacks                	map[string]int
	Available_memory 			int
	Available_disk 				int
	App_id_to_count				map[string]int
	TimeOfLastUpdate		    time.Time
	DockerVm					bool
}

//资源调度返回的dea数据格式
type FindDeaData struct {
	OwnerDeaIds				[]string
	DeaIds					string
}

//按照dea可用内存升序排序
type sortDeas struct{
	deas                []*DeaAdvertisement
	owners              []*DeaAdvertisement
}

//实现排序接口
func (s sortDeas) Len() int{
	return len(s.deas)
}

//排序规则接口
func (s sortDeas) Less(i, j int) bool{
	return s.deas[i].Available_memory < s.deas[j].Available_memory
}
//排序接口
func (s sortDeas) Swap(i, j int){
	s.deas[i],s.deas[j] = s.deas[j],s.deas[i]
}

//启动deapool 池
func (p *DeaPool) Start(){

	p.subDeaAdvertise()
	p.subDeaShutdown()
	p.deaResourceDispatch()
	p.startPruningCycle()
	
}

//启动dea资源超时检测
func (p *DeaPool) startPruningCycle(){
	
	if p.timeOutSecond >0 { //超时时间秒
		p.lock.Lock()
		p.ticker = time.NewTicker(p.timeOutThreshold)
		p.lock.Unlock()
		
		go func() {
			for {
				select {
				case <-p.ticker.C:
					p.logger.Info("Start to check and prune stale dea resources")
					p.pruneStaleDeaResources()
				}
			}
		}()
	}
}

//定时检测dea上报的资源是否超时
func (p *DeaPool) pruneStaleDeaResources(){
	p.lock.Lock()
	defer p.lock.Unlock()
	
	pruneTime := time.Now().Add(-p.timeOutThreshold)
	
	//遍历数据
	for _, val := range p.endpoints {
		
		//判断资源是否过期
		if val.TimeOfLastUpdate.Before(pruneTime) {
			p.logger.Infof("dea 资源调度,当前dea已经超时上报了, dea_info:%v ",val)
			delete(p.endpoints,val.Id)
		}
		
	}
	
}

//检测当前dea资源信息
func (p *DeaPool) CheckDeaPool() []byte{
	
	p.lock.Lock()
	defer p.lock.Unlock()
	
	a,_  := json.Marshal(p.endpoints)
	
	return a
}

//dea资源调度,监听 dea.resource.dispatch
//参数格式: {"appId":"0001","memory":10,"disk":10,"stacks":"linux","ownerApp":true,"otherDea":false}
//返回值格式:{"OwnerDeaIds":["0000000001","0000000002"],"DeaIds":"0000000003"}
func (p *DeaPool) deaResourceDispatch() {
	p.messageBus.SubscribeWithQueue("dea.resource.dispatch", "QUEUE_DISPATCH", func(message *yagnats.Message) {
	
		start := time.Now()
		
		payload := message.Payload
		var msg FindDeaMessage

		err := json.Unmarshal(payload, &msg)
		if err != nil {
			logMessage := fmt.Sprintf("%s: Error unmarshalling JSON (%d; %s): %s", "dea.resource.dispatch", len(payload), payload, err)
			p.logger.Warnd(map[string]interface{}{"payload": string(payload)}, logMessage)
		}
		
		response := p.FindDea(&msg)
		a ,err := json.Marshal(response)
		
		if err != nil {
			p.logger.Errorf("dea.resource.dispatch ,json.Marshal response err:%v",err)
		}
		p.messageBus.Publish(message.ReplyTo, a)
		
		end := time.Now()
		p.logger.Infof("dea资源调度,deaResourceDispatch,return dea_id:%v 耗时:%v ",response.DeaIds, end.Sub(start))
	})
}

//根据条件选择资源最优的dea
func (p *DeaPool) FindDea(message *FindDeaMessage) *FindDeaData{
	
	result := &FindDeaData{}
	
	sortdeas := p.validateDdeas(message)
	
	for _, val := range sortdeas.owners {
		result.OwnerDeaIds = append(result.OwnerDeaIds, val.Id)
	}
	
	//top 5 随机1 
	
	count := len(sortdeas.deas)
	p.logger.Infof("dea资源调度,满足条件的dea个数:%v",count)
	
	if count <= 0 {
		return result
	}
	
	if count >1 {//排序
		start := time.Now()
		sort.Sort(sortdeas)
		p.logger.Infof("dea资源调度,排序 耗时:%v ",time.Now().Sub(start))
	}
	
	if count == 1{
		result.DeaIds = sortdeas.deas[0].Id
	}else if count >5 {
		result.DeaIds = sortdeas.deas[count - p.top5(count/2) -1].Id
	}else{
		result.DeaIds = sortdeas.deas[p.top5(count - 1)].Id
	}
	
	return result
}


//生成一个top5的随机数
func (p *DeaPool) top5(endnum int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	i := r.Intn(endnum)
	p.logger.Debugf("dea资源调度产生的随机数:%v",i)
	
	return i
}

func (p *DeaPool) validateDdeas(message *FindDeaMessage) *sortDeas{
	
	start := time.Now()
	p.logger.Debugf("begin find dea validateDdeas ,param:%v , dea_count:%v",message,len(p.endpoints))
	
	appId 				:= message.AppId
	stacks 				:= message.Stacks
	available_memory 	:= message.Memory
	ownerApp          	:= message.OwnerApp //是否返回当前应用所在dea
	otherDea          	:= message.OtherDea // 是否需要排除应用运行所在的dea
	docker 				:= message.Docker //是否需要部署在docker上
	//超时时间
	pruneTime := time.Now().Add(-p.timeOutThreshold)
	
	p.lock.Lock()
	defer p.lock.Unlock()
	
	result := &sortDeas{}
	
	//遍历数据
	for _, val := range p.endpoints {
	
		if ownerApp {//要返回当前应用运行的dea
			_ , found := val.App_id_to_count[appId]
			if found {
				result.owners = append(result.owners, val)
			}
			continue
		}
		
		//判断是否满足docker要求
		if docker && !val.DockerVm {
			continue
		}
		//判断资源是否符合规则
		if val.Available_memory <= available_memory {//memory
			continue
		} 
		
		if val.Available_disk <= 1024 {//disk
			continue
		}
		
		if stacks != ""{
			_ , found := val.ByStacks[stacks]
			if !found {
				continue
			}
		}
		
		if otherDea {
			_ , found := val.App_id_to_count[appId]
			if found {
				continue
			}
		}
//		//判断资源是否过期
		if val.TimeOfLastUpdate.Before(pruneTime) {
			p.logger.Infof("dea 资源调度,当前dea已经超时上报了, dea_info:%v ",val)
			continue
		}
		
		result.deas = append(result.deas, val)
		
	}
	p.logger.Infof("dea资源调度,validateDdeas 耗时:%v ",time.Now().Sub(start))
	return result
}

func (p *DeaPool) subDeaAdvertise(){
	p.subscribeRegister("dea.advertise", func(itemMessage *poolItemMessage){
		p.logger.Debugf("Got dea.advertise:%v", itemMessage)
		p.register(itemMessage)
	})
}

func (p *DeaPool) subDeaShutdown(){
	p.subscribeShutdown("dea.shutdown", func(itemMessage *poolItemDownMessage){
		p.logger.Debugf("Got dea.shutdown:%v", itemMessage)
		p.unRegister(itemMessage)
	})
}

//注册一个dea的资源信息
func (p *DeaPool) register(itemMessage *poolItemMessage){

	t := time.Now()
	p.lock.Lock()
	defer p.lock.Unlock()
	
	deaid := itemMessage.Id
	
	_, found := p.endpoints[deaid]
	
	if found {//已经存在
		delete(p.endpoints, deaid)
	}
	
	stacksMap := make(map[string]int)
	
	for _, stack := range itemMessage.Stacks {
		stacksMap[stack] =1
	}
	
	deaObj := &DeaAdvertisement{
		Id:						itemMessage.Id,
		Stacks:              	itemMessage.Stacks,
		ByStacks:            	stacksMap,
		Available_memory: 		itemMessage.Available_memory,
		Available_disk: 		itemMessage.Available_disk,
		App_id_to_count:		itemMessage.AppIdToCount,
		TimeOfLastUpdate:		t,
		DockerVm:				itemMessage.DockerVm,
	}
	
	p.endpoints[deaid] = deaObj
	
}

//remove 一个已经注册过的dea信息
func (p *DeaPool) unRegister(itemMessage *poolItemDownMessage){
	
	p.lock.Lock()
	defer p.lock.Unlock()
	
	deaid := itemMessage.Id
	
	_, found := p.endpoints[deaid]
	
	if found {//已经存在
		delete(p.endpoints, deaid)
	}
}


//注册nats信息,收集dea上报的数据
func (p *DeaPool) subscribeRegister(subject string, successCallback func(*poolItemMessage)){

	callback := func(message *yagnats.Message) {
		payload := message.Payload

		var msg poolItemMessage

		err := json.Unmarshal(payload, &msg)
		if err != nil {
			logMessage := fmt.Sprintf("%s: Error unmarshalling JSON (%d; %s): %s", subject, len(payload), payload, err)
			p.logger.Warnd(map[string]interface{}{"payload": string(payload)}, logMessage)
		}

		logMessage := fmt.Sprintf("%s: Received message", subject)
		p.logger.Debugd(map[string]interface{}{"message": msg}, logMessage)

		successCallback(&msg)
	}

	_, err := p.messageBus.Subscribe(subject, callback)
	if err != nil {
		p.logger.Errorf("Error subscribing to %s: %s", subject, err)
	}

}


//注册nats信息,收集dea shutdown的数据
func (p *DeaPool) subscribeShutdown(subject string, successCallback func(*poolItemDownMessage)){

	callback := func(message *yagnats.Message) {
		payload := message.Payload

		var msg poolItemDownMessage

		err := json.Unmarshal(payload, &msg)
		if err != nil {
			logMessage := fmt.Sprintf("%s: Error unmarshalling JSON (%d; %s): %s", subject, len(payload), payload, err)
			p.logger.Warnd(map[string]interface{}{"payload": string(payload)}, logMessage)
		}

		logMessage := fmt.Sprintf("%s: Received message", subject)
		p.logger.Debugd(map[string]interface{}{"message": msg}, logMessage)

		successCallback(&msg)
	}

	_, err := p.messageBus.Subscribe(subject, callback)
	if err != nil {
		p.logger.Errorf("Error subscribing to %s: %s", subject, err)
	}
}

