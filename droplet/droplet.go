package droplet

import (
	"scheduler/config"
	"scheduler/util"
	"time"
	"sync"
	"net/http"
	"encoding/json"
	"os"
	"io"
	"syscall"
	"path/filepath"
	"errors"
	"fmt"
	steno "github.com/cloudfoundry/gosteno"
)

//droplet 上传下载管理对象
type DropletMgm struct {
	cache_base_dir			string
	cache_directory			string
	cache_time_out 			int
	cache_droplets			map[string]*Droplet
	cache_interval			int
	disk_mak_used_space  	int
	ticker 		       		*time.Ticker
	logger              	*steno.Logger
	jssUtil					*util.JssUtil
	lock 					sync.Mutex
}

//droplet 对象
type Droplet struct {
	AppGuid					string
	CachePath				string
	TimeOfLastUpdate		time.Time
}

//创建droplet 管理对象
func NewDropletMgm (c *config.Config) *DropletMgm {
	
	return &DropletMgm{
		cache_base_dir: c.Droplet.CacheBaseDir,
		cache_directory: c.Droplet.CacheDirecotry,
		cache_time_out: c.Droplet.CacheTimeOut,
		cache_droplets: make(map[string] *Droplet),
		cache_interval: c.Droplet.CacheInterval,
		disk_mak_used_space: c.Droplet.DiskMaxUsedSpace,
		logger:steno.NewLogger("cc_helper"),
		jssUtil: util.NewJssUtil(c, true),
	}
}

//返回当前内存中的所有droplets 格式化成 json
func (d *DropletMgm) CacheDroplets() []byte{
	d.lock.Lock()
	defer d.lock.Unlock()
	
	a,_  := json.Marshal(d.cache_droplets)
	
	return a
}

/**
	检测缓存使用情况,清空过期的数据
	缓存清理规则
	1. 首先检测内存中过期没有下载的droplet
	2.判断磁盘空间使用量占用百分比是否超过限制,清空缓存中创建时间最老的100个droplet
**/
func (d *DropletMgm) CheckCacheDroplet(){
	d.logger.Infof("process CheckCacheDroplet............")
	
	d.cleanCacheDroplet()
	
	//检测缓存磁盘大小
	diskAll , diskFree, usedPercent := d.diskStatus(d.cache_base_dir)
	d.logger.Infof("process CheckCacheDroplet check disk, all:%v, free:%v,usedPercent:%v",diskAll, diskFree, usedPercent)
	
	if usedPercent < int64(d.disk_mak_used_space) {
		return		
	}
	
	//过期时间
	pruneTime := time.Now().Add(- (time.Duration(d.cache_time_out) * time.Second))
	
	d.logger.Infof("process CheckCacheDroplet check disk used_percent:%v,超过了最大使用百分比:%v",usedPercent, d.disk_mak_used_space)
	
	filepath.Walk(d.cache_base_dir+"/"+d.cache_directory,func(path string, fi os.FileInfo, err error) error{
	
		if nil == fi {  
			return nil  
		} 
		
		if !fi.IsDir() {//只判断文件
			mdtime := fi.ModTime()
			//判断时间是否过期
			if mdtime.Before(pruneTime) {
				d.logger.Infof("process CheckCacheDroplet remove timeout droplet ,path:%v",path)
				os.Remove(path)
			}
			
		}
		return nil
	})
	
}

//从缓存中清空超时未使用的droplet
func (d *DropletMgm) cleanCacheDroplet() {
	d.logger.Infof("process CheckCacheDroplet cleanCacheDroplet.......")
	
	//过期时间
	pruneTime := time.Now().Add(- (time.Duration(d.cache_time_out) * time.Second))
	
	d.lock.Lock()
	defer d.lock.Unlock()
	
	for key, droplet := range d.cache_droplets {
		
		if droplet.TimeOfLastUpdate.Before(pruneTime) {
			d.logger.Infof("cache droplet time out and remove local cache ,droplet:%v",droplet)
			os.Remove(droplet.CachePath)
			delete(d.cache_droplets, key)
			continue
		}
	}
}

//获取磁盘情况,总大小,空闲大小,占用百分比
func (p *DropletMgm) diskStatus(path string) (uint64,uint64,int64){
	fs := syscall.Statfs_t{}
    err := syscall.Statfs(path, &fs)
    if err != nil {
    	p.logger.Infof("process CheckCacheDroplet diskStatus fail: %v",err)
        return 0,0,0
    }
    
    all := fs.Blocks * uint64(fs.Bsize)
    free := fs.Bfree * uint64(fs.Bsize)
    used := all - free
    
    return all,free,int64(float64(used)/float64(all)*100)
}


//根据guid删除本地缓存数据
func (p *DropletMgm) DestoryDroplet(guid string) {

	p.logger.Infof("delete app droplet,guid:%v,from cache ",guid)
	dropletPath,cache := p.getCacheDroplet(guid)
	if !cache {
		dropletPath = p.getCachePath(guid)+"/"+guid
		os.Remove(dropletPath)
	}else{
		os.Remove(dropletPath)
	}
	p.unRegisterCache(guid)
}

/**
	上传droplet
	dea在打包完成后会调用该接口进行上传打好的droplet
	1:首先将接受到的droplet保存到本地磁盘(缓存盘)
	2:注册cache
	3:上传到jss
*/
func (d *DropletMgm) UploadDroplet(rw http.ResponseWriter, req *http.Request, vars map[string]string) error{

	start := time.Now()
	method := req.Method
	params := req.URL.Query()
	guid := params.Get(":guid")
	
	if guid == "" {
		d.logger.Error("upload droplet,guid is empty")
		rw.WriteHeader(400);
		return errors.New("upload droplet,guid is empty")
	}
	
	d.logger.Infof("upload droplet,guid:%v, method:%v",guid,method)
	
	//解析上传文件
	reader,err := req.MultipartReader()
	
	if err != nil {
		d.logger.Errorf("upload droplet,guid:%v, method:%v,fail: %v",guid,method,err)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("upload droplet,guid:%v, method:%v,fail: %v",guid,method,err) )
	}
	
	//检测缓存目录是否存在
	cachePath := d.getCachePath(guid)
	dir ,err := os.Stat(cachePath)
	
	if err != nil || !dir.IsDir() {//目录不存在需要创建
		err := os.MkdirAll(cachePath , 0777)
		if err != nil {
			d.logger.Errorf("upload droplet,guid:%v, method:%v,create cache dir fail, cache Path: %v, err:%v", guid, method, cachePath, err)
			rw.WriteHeader(400);
			return errors.New(fmt.Sprintf("upload droplet,guid:%v, method:%v,create cache dir fail, cache Path: %v, err:%v", guid, method, cachePath, err) )
		}
	}
	
	//保存droplet 到 cache
	filePath := cachePath+"/"+guid
	d.checkCacheFileAndRemove(filePath)
	
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	
	if err !=nil {
		d.logger.Errorf("upload droplet,guid:%v, method:%v,create droplet file fail,file: %v, err:%v", guid, method, filePath, err)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("upload droplet,guid:%v, method:%v,create droplet file fail,file: %v, err:%v", guid, method, filePath, err) )
	}
	
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if part == nil {
			break
		}
		for {
			buffer := make([]byte,100000)
			cBytes,err := part.Read(buffer)
			
			if err == io.EOF {
				break
			}
			
			f.Write(buffer[0:cBytes])
		}
	}
	
	err = f.Close()
	if err != nil {
		d.logger.Errorf("upload droplet,guid:%v, fail: %v", guid, err)
		return errors.New(fmt.Sprintf("upload droplet,guid:%v, fail: %v", guid, err) )
	}
	
	//上传文件到jss(如果上传失败 清空本地缓存,通知dea上传失败)
	jssupload := d.jssUtil.Upload(guid, filePath)
	if !jssupload {
	 os.Remove(filePath)
	 d.logger.Errorf("upload droplet,guid:%v, method:%v,upload to jss fail,clean cache file,file: %v", guid, method, filePath)
	 rw.WriteHeader(400);
	 return errors.New(fmt.Sprintf("upload droplet,guid:%v, method:%v,upload to jss fail,clean cache file,file: %v", guid, method, filePath) )
	}
	
	//注册缓存信息
	d.registerCache(guid, filePath)
	
	end := time.Now()
	d.logger.Infof("upload droplet,guid:%v, method:%v, success, 耗时:%v",guid, method,  end.Sub(start))
	
	rw.WriteHeader(200);
	
	return nil
}

/**
* 下载droplet
 1：首先检测本地缓存中是否存在,如果存在则从缓存中获取直接返回
 2:如果缓存中不存在就从JSS 下载到本地缓存,然后再返回到dea
*/
func (d *DropletMgm) DownloadDroplet(rw http.ResponseWriter, req *http.Request, vars map[string]string) error {
	
	start := time.Now()
	method := req.Method
	params := req.URL.Query()
	guid := params.Get(":guid")
	
	if guid == "" {
		d.logger.Error("download droplet,guid is empty")
		rw.WriteHeader(400);
		return errors.New("download droplet,guid is empty")
	}
	
	var dropletPath string
	d.logger.Infof("download droplet,guid:%v, method:%v",guid,method)
	
	dropletPath,cache := d.getCacheDroplet(guid)
	if !cache {
		d.logger.Infof("download droplet,guid:%v, method:%v,not found from cache ",guid,method)
		dropletPath := d.getCachePath(guid)+"/"+guid
		
		//判断path在cache中是否存在(内存中的数据可能不准确,比如服务重启等情况)
		if _, err := os.Stat(dropletPath); err == nil {
	        d.logger.Infof("found file:%v, from cache disk ",dropletPath)
	        d.registerCache(guid, dropletPath)
	    }else{
		    //从jss中下载
			jssdownload := d.jssUtil.Download(guid, dropletPath,rw)
			if !jssdownload {
				d.logger.Errorf("download droplet fail, cache not found and downlaod from jss fail,guid:%v,dropletpath:%v",guid, dropletPath)
				rw.WriteHeader(400);
				return errors.New(fmt.Sprintf("download droplet fail, cache not found and downlaod from jss fail,guid:%v,dropletpath:%v",guid, dropletPath) )
			}
			rw.WriteHeader(200);
			d.registerCache(guid, dropletPath)
			end := time.Now()
    		d.logger.Infof("download droplet,guid:%v, path:%v, success, 耗时:%v",guid, dropletPath,  end.Sub(start))
			return nil
	    }
	}
	//send file
	http.ServeFile(rw,req,dropletPath)
    end := time.Now()
    d.logger.Infof("download droplet,guid:%v, path:%v, success, 耗时:%v",guid, dropletPath,  end.Sub(start))
    return nil
}


//检测本地磁盘中缓存的文件是否存在,如果存在则删除该文件
func (d *DropletMgm) checkCacheFileAndRemove(path string) {

	if _, err := os.Stat(path); err == nil {
        d.logger.Infof("found file:%v, from cache disk and remove this",path)
        os.Remove(path)
    }
}

//根据guid 从缓存中获取droplet 路径
func (d *DropletMgm) getCacheDroplet(guid string) (string, bool){
	d.lock.Lock()
	defer d.lock.Unlock()
	
	droplet, found := d.cache_droplets[guid]
	
	if found {//已经存在
		delete(d.cache_droplets, guid)
		if _, err := os.Stat(droplet.CachePath); err == nil {
			droplet.TimeOfLastUpdate = time.Now()
			d.cache_droplets[guid] = droplet
          return droplet.CachePath,true
    	}
	}
	
	return "",false
}

func (d *DropletMgm) unRegisterCache(guid string) {

	d.logger.Infof("unRegister droplet cache ,guid:%v",guid)
	
	d.lock.Lock()
	defer d.lock.Unlock()
	
	_, found := d.cache_droplets[guid]
	
	if found {//已经存在
		delete(d.cache_droplets, guid)
	}
}

//注册缓存的 droplet 到内存缓存
func (d *DropletMgm) registerCache(guid string, path string) {

	d.logger.Infof("register droplet cache ,guid:%v,path:%v",guid, path)
	
	d.lock.Lock()
	defer d.lock.Unlock()
	
	_, found := d.cache_droplets[guid]
	
	if found {//已经存在
		delete(d.cache_droplets, guid)
	}
	
	cacheDroplet := &Droplet{AppGuid:guid,CachePath:path,TimeOfLastUpdate:time.Now(),}
	d.cache_droplets[guid] = cacheDroplet
}

//根据guid返回 缓存文件在本地磁盘的路径
func (d *DropletMgm) getCachePath(guid string) string {
	
	rs := [] rune(guid)
	
	if len(rs) < 4{
		return ""
	}
	
	path := d.cache_base_dir+"/"+d.cache_directory+"/"+string(rs[0:2])+"/"+string(rs[2:4])
	
	//检测目录是否存在
	dir ,err := os.Stat(path)
	if err != nil || !dir.IsDir() {//目录不存在需要创建
		err := os.MkdirAll(path , 0777)
		if err != nil {
			d.logger.Errorf("mkdir path:%v fail,%v", path, err)
			return ""
		}
	}
	
	return path
}