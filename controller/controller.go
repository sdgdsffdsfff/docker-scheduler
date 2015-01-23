package controller

import (
	"net/http"
	"scheduler/config"
	"scheduler/deapool"
	steno "github.com/cloudfoundry/gosteno"
	"github.com/gorilla/mux"
	"net"
	"os"
	"scheduler/droplet"
	"scheduler/apppackage"
	"scheduler/buildpackcache"
	"strconv"
	"encoding/json"
)

type Controller struct {
   
   cfg      		*config.Config
   deaPool 			*deapool.DeaPool
   droplet			*droplet.DropletMgm
   packages			*apppackage.PackageMgm
   buildpack		*buildpackcache.BuildpackMgm
   logger         	*steno.Logger
}

type HttpApiFunc func(w http.ResponseWriter, r *http.Request, vars map[string]string) error

//创建一个controller对象
func NewController(config *config.Config, pool *deapool.DeaPool, dropletMgm *droplet.DropletMgm, packageMgm *apppackage.PackageMgm, buildpackmgm *buildpackcache.BuildpackMgm) *Controller{
	return &Controller{
		cfg:			config,
		deaPool:      	pool,
		droplet:		dropletMgm,
		packages:		packageMgm,
		buildpack:		buildpackmgm,
		logger: 		steno.NewLogger("cc_helper"),
	}
}

func (c *Controller) returnJson(v interface{}, w http.ResponseWriter) error{
	data, err := encodeJson(v)
	
	if err != nil {
		c.logger.Errorf("encodejson fail,err:%v",err)
		return err
	}else {
		writeJson(data, w)
	}
	return nil
}

func (c *Controller) makeHttpHandler(logging bool, localMethod string, localRouter string, handlerFunc HttpApiFunc) http.HandlerFunc {
	
	return func(w http.ResponseWriter, r *http.Request) {
		c.logger.Infof("Calling %s %s", localMethod, localRouter)
		
		if logging {
			c.logger.Infof("reqMethod:%s , reqURI:%s , userAgent:%s", r.Method, r.RequestURI, r.Header.Get("User-Agent"))
		}
		
		if err := handlerFunc(w , r, mux.Vars(r)) ; err != nil {
			c.logger.Errorf("Handler for %s %s returned error: %s", localMethod, localRouter, err)
			http.Error(w, err.Error(), 400)
		}
	}
}


func (c *Controller) createoRuter () (*mux.Router, error) {
	r := mux.NewRouter()
	
	m := map[string]map[string] HttpApiFunc {
		"GET": {
			"/test":																c.testHandler,
			"/health":																c.healthHandler,
			"/deapool":																c.deaPoolHandler,	
			"/droplets":															c.dropletsHandler,	
			"/packages":															c.packagesHandler,	
			"/buildpackcache":														c.packagesHandler,	
			"/droplet/{guid}/download":												c.droplet.DownloadDroplet,		
			"/packages/{guid}/download":											c.packages.DownloadPackage,
			"/buildpackCache/{guid}/download":										c.buildpack.DownloadBuildpack,	
			"/{appid}/{memory}/{disk}/{stacks}/{owner}/{other}/{docker}/finddea":	c.findDea,
		},
		"POST": {
			"/droplet/{guid}/upload":			c.droplet.UploadDroplet,
			"/packages/{guid}/upload":			c.packages.UploadPackage,
			"/buildpackCache/:guid/upload":		c.buildpack.UploadBuildpack,
		},
		"DELETE": {
		
		},
		"PUT": {
		
		},
	}
	
	//遍历定义的方法,注册服务
	for method, routers := range m {
		
		for route, fct := range routers {
			c.logger.Infof("registering method:%s, router:%s", method, route)
			
			localRoute := route
			localFct   := fct
			localMethod := method
			
			//build the handler function
			f := c.makeHttpHandler(true, localMethod, localRoute, localFct)
			
			if localRoute == "" {
				r.Methods(localMethod).HandlerFunc(f)
			}else {
				r.Path("/" + "scheduler"+ localRoute).Methods(localMethod).HandlerFunc(f)
			}
		}
	}
	
	return r, nil
}


// 开启服务监听
func (c *Controller) listenAndServe() error {
	
	var l net.Listener
	r, err := c.createoRuter()
	
	if err != nil {
		return err
	}
	
	addr := ":"+c.cfg.Port
	
	l, err  = net.Listen("tcp", addr)
	if err != nil {
		c.logger.Errorf("listenAndServe fail, %s", err)	
		return err
	}
	httpSrv := http.Server{Addr: addr, Handler: r}
	
	return httpSrv.Serve(l)
}

func (c *Controller) Start() {
	c.logger.Infof("starting service ........")
	err :=  c.listenAndServe()
	if err != nil {
		c.logger.Errorf("ServeApi error , %s", err)
		os.Exit(1)
	}
	c.logger.Infof("starting service success, port: %s", c.cfg.Port)
}

//查询可用的dea
func (c *Controller) findDea(w http.ResponseWriter, r *http.Request, vars map[string]string) error {
	appid 		:= vars["appid"]
	memoryStr		:= vars["memory"]
	diskStr		:= vars["disk"]
	stacks		:= vars["stacks"]
	ownerStr	:= vars["owner"]
	otherStr	:= vars["other"]
	dockerStr	:= vars["docker"]
	
	owner 		:= false
	other		:= false
	docker		:= false
	memory		:= 256
	disk		:= 512
	
	v , err :=strconv.Atoi(memoryStr)
	if err == nil {
		memory = v	
	}
	v, err = strconv.Atoi(diskStr)
	if err == nil {
		disk = v
	}	
	val , err := strconv.ParseBool(ownerStr)
	if err == nil {
		owner = val
	}
	val , err = strconv.ParseBool(otherStr)
	if err == nil {
		other = val
	}
	val , err = strconv.ParseBool(dockerStr)
	if err == nil {
		docker = val
	}
	
	findMesg :=  &deapool.FindDeaMessage{
		AppId:			appid,
		Memory:			memory,
		Disk:			disk,
		Stacks:			stacks,
		OwnerApp:		owner,
		OtherDea:		other,
		Docker:			docker,
	}
	
	a, err := json.Marshal(findMesg)
	if err != nil {
		c.logger.Errorf("call find dea fail, parameter fail:%s", err)
		return err
	}
	
	c.logger.Infof("begin call find dea, paramteer:%s", string(a) )
	
	dea := c.deaPool.FindDea(findMesg)
	c.returnJson(dea, w)
	return nil
}

func (c *Controller) testHandler(w http.ResponseWriter, r *http.Request, vars map[string]string) error {
	w.WriteHeader(403)
	return nil
}

func (c *Controller) deaPoolHandler(w http.ResponseWriter, r *http.Request, vars map[string]string) error {
	c.returnJson(c.deaPool.CheckDeaPool(), w)
	return nil
}

//cache 中 buildpackcache 查询
func (c *Controller) buildpackCacheHandler(w http.ResponseWriter, r *http.Request, vars map[string]string) error {
	c.returnJson(c.buildpack.CacheBuildpacks(), w)
	return nil
}

//cache 中 packages 查询
func (c *Controller) packagesHandler(w http.ResponseWriter, r *http.Request, vars map[string]string) error{
	c.returnJson(c.packages.CachePackages(), w)
	return nil
}

//cache中 droplet 查询
func (c *Controller) dropletsHandler(w http.ResponseWriter, r *http.Request, vars map[string]string) error {
	c.returnJson(c.droplet.CacheDroplets(), w)
	return nil
}

//存活检测
func (c *Controller) healthHandler(w http.ResponseWriter, r *http.Request, vars map[string]string) error{
	health := &responseHealth{Status:"ok"}
	c.returnJson(health, w)
	return nil
}

type responseHealth struct {
	Status			string
}