package buildpackcache

import (
	"scheduler/config"
	"time"
	"sync"
	"net/http"
	"encoding/json"
	"os"
	"io"
	"errors"
	"fmt"
	"syscall"
	"path/filepath"
	steno "github.com/cloudfoundry/gosteno"
)

//buildpack 上传下载管理对象
type BuildpackMgm struct {
	cache_base_dir		string
	cache_directory		string
	cache_time_out 		int
	cache_buildpacks		map[string]*Buildpack
	cache_interval		int
	disk_mak_used_space  int
	ticker 		       *time.Ticker
	logger               *steno.Logger
	lock 					sync.Mutex
}

//buildpack 对象
type Buildpack struct {
	AppGuid				string
	CachePath				string
	TimeOfLastUpdate		time.Time
}

//buildpack 管理对象
func NewBuildpackMgm (c *config.Config) *BuildpackMgm {
	
	return &BuildpackMgm{
		cache_base_dir: c.Buildpack.CacheBaseDir,
		cache_directory: c.Buildpack.CacheDirecotry,
		cache_time_out: c.Buildpack.CacheTimeOut,
		cache_buildpacks: make(map[string] *Buildpack),
		cache_interval: c.Droplet.CacheInterval,
		disk_mak_used_space: c.Droplet.DiskMaxUsedSpace,
		logger:steno.NewLogger("cc_helper"),
	}
}

//返回当前内存中的所有droplets 格式化成 json
func (b *BuildpackMgm) CacheBuildpacks() []byte{
	b.lock.Lock()
	defer b.lock.Unlock()
	
	a,_  := json.Marshal(b.cache_buildpacks)
	
	return a
}

/**
	检测缓存使用情况,清空过期的数据
	缓存清理规则
	1. 首先检测内存中过期没有下载的buildpack cache
	2.判断磁盘空间使用量占用百分比是否超过限制,清空缓存中创建时间最老的buildpack cache
**/
func (b *BuildpackMgm) CheckCacheBuildpack(){
	b.logger.Infof("process CheckBuildpackCache............")
	
	b.cleanCacheBuildpack()
	
	//检测缓存磁盘大小
	diskAll , diskFree, usedPercent := b.diskStatus(b.cache_base_dir)
	b.logger.Infof("process CheckCache buildpackCache check disk, all:%v, free:%v,usedPercent:%v",diskAll, diskFree, usedPercent)
	
	if usedPercent < int64(b.disk_mak_used_space) {
		return		
	}
	
	//过期时间
	pruneTime := time.Now().Add(- (time.Duration(b.cache_time_out) * time.Second))
	
	b.logger.Infof("process CheckCache buildpack Cache check disk used_percent:%v,超过了最大使用百分比:%v",usedPercent, b.disk_mak_used_space)
	
	filepath.Walk(b.cache_base_dir+"/"+b.cache_directory,func(path string, fi os.FileInfo, err error) error{
	
		if nil == fi {  
			return nil  
		} 
		
		if !fi.IsDir() {//只判断文件
			mdtime := fi.ModTime()
			//判断时间是否过期
			if mdtime.Before(pruneTime) {
				b.logger.Infof("process CheckCache buildpack Cache remove timeout buildpack ,path:%v",path)
				os.Remove(path)
			}
			
		}
		return nil
	})
	
}

//从缓存中清空超时未使用的 buildpackcache
func (b *BuildpackMgm) cleanCacheBuildpack() {
	b.logger.Infof("process CheckCacheBuildpackcache cleanCache.......")
	
	//过期时间
	pruneTime := time.Now().Add(- (time.Duration(b.cache_time_out) * time.Second))
	
	b.lock.Lock()
	defer b.lock.Unlock()
	
	for key, buildpack := range b.cache_buildpacks {
		
		if buildpack.TimeOfLastUpdate.Before(pruneTime) {
			b.logger.Infof("cache buildpack cache time out and remove local cache ,buildpack:%v",buildpack)
			os.Remove(buildpack.CachePath)
			delete(b.cache_buildpacks, key)
			continue
		}
	}
}

//获取磁盘情况,总大小,空闲大小,占用百分比
func (b *BuildpackMgm) diskStatus(path string) (uint64,uint64,int64){
	fs := syscall.Statfs_t{}
    err := syscall.Statfs(path, &fs)
    if err != nil {
    	b.logger.Infof("process CheckCache buildpack  diskStatus fail: %v",err)
        return 0,0,0
    }
    
    all := fs.Blocks * uint64(fs.Bsize)
    free := fs.Bfree * uint64(fs.Bsize)
    used := all - free
    
    return all,free,int64(float64(used)/float64(all)*100)
}

//根据guid删除本地缓存数据
func (b *BuildpackMgm) DestoryBuildpack(guid string) {

	b.logger.Infof("delete app buildpack,guid:%v,from cache ",guid)
	buildpackPath,cache := b.getCacheBuildpack(guid)
	if !cache {
		buildpackPath = b.getCachePath(guid)+"/"+guid
		os.Remove(buildpackPath)
	}else {
		os.Remove(buildpackPath)
	}
	b.unRegisterCache(guid)
}

/**
	上传buildpack
	dea在打包过程中会针对不同的应用缓存一些包的依赖项
	1:首先将接受到的cache保存到本地磁盘(缓存盘)
	2:注册cache
*/
func (b *BuildpackMgm) UploadBuildpack(rw http.ResponseWriter, req *http.Request, vars map[string]string) error {

	start := time.Now()
	method := req.Method
	params := req.URL.Query()
	guid := params.Get(":guid")
	
	if guid == "" {
		b.logger.Error("upload buildpackCache,guid is empty")
		rw.WriteHeader(400);
		return errors.New("upload buildpackCache,guid is empty")
	}
	
	b.logger.Infof("upload buildpackCache,guid:%v, method:%v",guid,method)
	
	//解析上传文件
	reader,err := req.MultipartReader()
	
	if err != nil {
		b.logger.Errorf("upload buildpackCache,guid:%v, method:%v,fail: %v",guid,method,err)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("upload buildpackCache,guid:%v, method:%v,fail: %v",guid,method,err) )
	}
	
	//检测缓存目录是否存在
	cachePath := b.getCachePath(guid)
	dir ,err := os.Stat(cachePath)
	
	if err != nil || !dir.IsDir() {//目录不存在需要创建
		err := os.MkdirAll(cachePath , 0777)
		if err != nil {
			b.logger.Errorf("upload buildpackCache,guid:%v, method:%v,create cache dir fail, cache Path: %v, err:%v", guid, method, cachePath, err)
			rw.WriteHeader(400);
			return errors.New(fmt.Sprintf("upload buildpackCache,guid:%v, method:%v,create cache dir fail, cache Path: %v, err:%v", guid, method, cachePath, err))
		}
	}
	
	//保存droplet 到 cache
	filePath := cachePath+"/"+guid
	b.checkCacheFileAndRemove(filePath)
	
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	
	if err !=nil {
		b.logger.Errorf("upload buildpackCache,guid:%v, method:%v,create droplet file fail,file: %v, err:%v", guid, method, filePath, err)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("upload buildpackCache,guid:%v, method:%v,create droplet file fail,file: %v, err:%v", guid, method, filePath, err) )
	}
	
	defer f.Close()
	
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
	
	//注册缓存信息
	b.registerCache(guid, filePath)
	
	end := time.Now()
	b.logger.Infof("upload buildpackCache,guid:%v, method:%v, success, 耗时:%v",guid, method,  end.Sub(start))
	
	rw.WriteHeader(200);
	
	return nil
}

/**
* 下载buildpack
 1：首先检测本地缓存中是否存在,如果存在则从缓存中获取直接返回
*/
func (b *BuildpackMgm) DownloadBuildpack(rw http.ResponseWriter, req *http.Request, vars map[string]string) error {
	
	start := time.Now()
	method := req.Method
	params := req.URL.Query()
	guid := params.Get(":guid")
	
	if guid == "" {
		b.logger.Error("download buildpackCache,guid is empty")
		rw.WriteHeader(400)
		return errors.New("download buildpackCache,guid is empty")
	}
	
	var buildpackCachePath string
	b.logger.Infof("download buildpackCache,guid:%v, method:%v",guid,method)
	
	buildpackCachePath,cache := b.getCacheBuildpack(guid)
	if !cache {
		b.logger.Infof("download buildpackCache,guid:%v, method:%v,not found from cache ",guid,method)
		buildpackCachePath := b.getCachePath(guid)+"/"+guid
		
		//判断path在cache中是否存在(内存中的数据可能不准确,比如服务重启等情况)
		if _, err := os.Stat(buildpackCachePath); err == nil {
	        b.logger.Infof("found file:%v, from cache disk ",buildpackCachePath)
	        b.registerCache(guid, buildpackCachePath)
	    }
	}
	
	if buildpackCachePath == "" {
		b.logger.Infof("download buildpackCache,guid:%v, method:%v,not found from cache ",guid,method)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("download buildpackCache,guid:%v, method:%v,not found from cache ",guid,method) )
	}
	
	//send file
	http.ServeFile(rw,req,buildpackCachePath)
    end := time.Now()
    b.logger.Infof("download buildpackCache,guid:%v, method:%v, success, 耗时:%v",guid, method,  end.Sub(start))
    return nil
}

//检测本地磁盘中缓存的文件是否存在,如果存在则删除该文件
func (b *BuildpackMgm) checkCacheFileAndRemove(path string) {

	if _, err := os.Stat(path); err == nil {
        b.logger.Infof("found file:%v, from cache disk and remove this",path)
        os.Remove(path)
    }
}

//注册缓存的 droplet 到内存缓存
func (b *BuildpackMgm) registerCache(guid string, path string) {

	b.logger.Infof("register buildpack cache ,guid:%v,path:%v",guid, path)
	
	b.lock.Lock()
	defer b.lock.Unlock()
	
	_, found := b.cache_buildpacks[guid]
	
	if found {//已经存在
		delete(b.cache_buildpacks, guid)
	}
	
	cacheBuildpack := &Buildpack{AppGuid:guid,CachePath:path,TimeOfLastUpdate:time.Now(),}
	b.cache_buildpacks[guid] = cacheBuildpack
}

//根据guid返回 缓存文件在本地磁盘的路径
func (b *BuildpackMgm) getCachePath(guid string) string {
	
	rs := [] rune(guid)
	
	if len(rs) < 4{
		return ""
	}
	
	path := b.cache_base_dir+"/"+b.cache_directory+"/"+string(rs[0:2])+"/"+string(rs[2:4])
	//检测目录是否存在
	dir ,err := os.Stat(path)
	if err != nil || !dir.IsDir() {//目录不存在需要创建
		err := os.MkdirAll(path , 0777)
		if err != nil {
			b.logger.Errorf("mkdir path:%v fail,%v", path, err)
			return ""
		}
	}
	
	return path
	
}

//根据guid 从缓存中获取droplet 路径
func (b *BuildpackMgm) getCacheBuildpack(guid string) (string, bool){
	b.lock.Lock()
	defer b.lock.Unlock()
	
	buildpack, found := b.cache_buildpacks[guid]
	
	if found {//已经存在
		delete(b.cache_buildpacks, guid)
		if _, err := os.Stat(buildpack.CachePath); err == nil {
			buildpack.TimeOfLastUpdate = time.Now()
			b.cache_buildpacks[guid] = buildpack
          return buildpack.CachePath,true
    	}
	}
	
	return "",false
}

func (b *BuildpackMgm) unRegisterCache(guid string) {

	b.logger.Infof("unRegister buildpack cache ,guid:%v",guid)
	
	b.lock.Lock()
	defer b.lock.Unlock()
	
	_, found := b.cache_buildpacks[guid]
	
	if found {//已经存在
		delete(b.cache_buildpacks, guid)
	}
}
