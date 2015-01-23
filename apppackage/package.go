package apppackage

import (
	"scheduler/config"
	"scheduler/util"
	"time"
	"sync"
	"net/http"
	"os"
	"io"
	"errors"
	"fmt"
	"encoding/json"
	steno "github.com/cloudfoundry/gosteno"
)


//应用源代码包上传下载管理
type PackageMgm struct{
	cache_base_dir			string
	cache_directory			string
	cache_time_out 			int
	cache_packages			map[string]*Package
	cache_interval			int
	disk_mak_free_space  	int
	ticker 		       		*time.Ticker
	logger               	*steno.Logger
	jssUtil					*util.JssUtil
	lock 					sync.Mutex
}

type Package struct{
	AppGuid					string
	CachePath				string
	TimeOfLastUpdate		time.Time
}

//创建一个package 管理对象
func NewPackageMgm(c *config.Config) *PackageMgm{
	return &PackageMgm{
		cache_base_dir: c.Package.CacheBaseDir,
		cache_directory: c.Package.CacheDirecotry,
		cache_time_out: c.Package.CacheTimeOut,
		cache_packages: make(map[string] *Package),
		cache_interval: c.Package.CacheInterval,
		disk_mak_free_space: c.Package.DiskMaxUsedSpace,
		logger:steno.NewLogger("cc_helper"),
		jssUtil: util.NewJssUtil(c, false),
	}
}

//返回当前内存中的所有droplets 格式化成 json
func (p *PackageMgm) CachePackages() []byte{
	p.lock.Lock()
	defer p.lock.Unlock()
	
	a,_  := json.Marshal(p.cache_packages)
	
	return a
}

//根据guid删除本地缓存数据
func (p *PackageMgm) DestoryPackage(guid string) {

	p.logger.Infof("delete app package,guid:%v,from cache ",guid)
	packagePath,cache := p.getCacheAppPackage(guid)
	if !cache {
		packagePath = p.getCachePath(guid)+"/"+guid
		os.Remove(packagePath)
	}else{
		os.Remove(packagePath)
	}
	p.unRegisterCache(guid)
}

/**
* 下载 package
 1：首先检测本地缓存中是否存在,如果存在则从缓存中获取直接返回
 2:如果缓存中不存在就从JSS 下载到本地缓存,然后再返回到dea
*/
func (p *PackageMgm) DownloadPackage(rw http.ResponseWriter, req *http.Request, vars map[string]string) error {
	
	start := time.Now()
	method := req.Method
	params := req.URL.Query()
	guid := params.Get(":guid")
	
	if guid == "" {
		p.logger.Error("download app package,guid is empty")
		rw.WriteHeader(400);
		return errors.New("download app package,guid is empty")
	}
	
	var packagePath string
	p.logger.Infof("download app package,guid:%v, method:%v",guid,method)
	
	 //从jss中下载
	packagePath = p.getCachePath(guid)+"/"+guid
	jssdownload := p.jssUtil.Download(guid, packagePath, rw)
	if !jssdownload {
		p.logger.Errorf("download app package fail, cache not found and downlaod from jss fail,guid:%v,dropletpath:%v",guid, packagePath)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("download app package fail, cache not found and downlaod from jss fail,guid:%v,dropletpath:%v",guid, packagePath) )
	}
	end := time.Now()
	p.logger.Infof("download app package,guid:%v, method:%v, success, 耗时:%v",guid, method,  end.Sub(start))
	return nil
}

//根据guid 从缓存中获取droplet 路径
func (p *PackageMgm) getCacheAppPackage(guid string) (string, bool){
	p.lock.Lock()
	defer p.lock.Unlock()
	
	appPackage, found := p.cache_packages[guid]
	
	if found {//已经存在
		delete(p.cache_packages, guid)
		if _, err := os.Stat(appPackage.CachePath); err == nil {
			appPackage.TimeOfLastUpdate = time.Now()
			p.cache_packages[guid] = appPackage
          return appPackage.CachePath,true
    	}
	}
	
	return "",false
}

/**
	上传 package
	dea在打包完成后会调用该接口进行上传打好的package
	1:首先将接受到的package保存到本地磁盘(缓存盘)
	2:注册cache
	3:上传到jss
*/
func (p *PackageMgm) UploadPackage(rw http.ResponseWriter, req *http.Request, vars map[string]string) error{

	start := time.Now()
	method := req.Method
	params := req.URL.Query()
	guid := params.Get(":guid")
	
	if guid == "" {
		p.logger.Error("upload app package,guid is empty")
		rw.WriteHeader(400);
		return errors.New("upload app package,guid is empty")
	}
	
	p.logger.Infof("upload app package,guid:%v, method:%v",guid,method)
	
	//解析上传文件
	reader,err := req.MultipartReader()
	
	if err != nil {
		p.logger.Errorf("upload app package,guid:%v, method:%v,fail: %v",guid,method,err)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("upload app package,guid:%v, method:%v,fail: %v",guid,method,err) )
	}
	
	//检测缓存目录是否存在
	cachePath := p.getCachePath(guid)
	dir ,err := os.Stat(cachePath)
	
	if err != nil || !dir.IsDir() {//目录不存在需要创建
		err := os.MkdirAll(cachePath , 0777)
		if err != nil {
			p.logger.Errorf("upload app package,guid:%v, method:%v,create cache dir fail, cache Path: %v, err:%v", guid, method, cachePath, err)
			rw.WriteHeader(400);
			return errors.New(fmt.Sprintf("upload app package,guid:%v, method:%v,create cache dir fail, cache Path: %v, err:%v", guid, method, cachePath, err) )
		}
	}
	
	//保存droplet 到 cache
	filePath := cachePath+"/"+guid
	p.checkCacheFileAndRemove(filePath)
	
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	
	if err !=nil {
		p.logger.Errorf("upload app package,guid:%v, method:%v,create package file fail,file: %v, err:%v", guid, method, filePath, err)
		rw.WriteHeader(400);
		return errors.New(fmt.Sprintf("upload app package,guid:%v, method:%v,create package file fail,file: %v, err:%v", guid, method, filePath, err) )
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
		p.logger.Errorf("upload package,guid:%v, fail: %v", guid, err)
		return errors.New(fmt.Sprintf("upload package,guid:%v, fail: %v", guid, err) )
	}
	
	//上传文件到jss(如果上传失败 清空本地缓存,通知dea上传失败)
	jssupload := p.jssUtil.Upload(guid, filePath)
	if !jssupload {
	 os.Remove(filePath)
	 p.logger.Errorf("upload app package,guid:%v, method:%v,upload to jss fail,clean cache file,file: %v", guid, method, filePath)
	 rw.WriteHeader(400);
	 return errors.New(fmt.Sprintf("upload app package,guid:%v, method:%v,upload to jss fail,clean cache file,file: %v", guid, method, filePath) )
	}
	
	//注册缓存信息
	p.registerCache(guid, filePath)
	
	end := time.Now()
	p.logger.Infof("upload app package,guid:%v, method:%v, success, 耗时:%v",guid, method,  end.Sub(start))
	
	rw.WriteHeader(200);
	
	return nil
}


func (p *PackageMgm) unRegisterCache(guid string){
	p.logger.Infof("unregister droplet cache ,guid:%v",guid)
	
	p.lock.Lock()
	defer p.lock.Unlock()
	
	_, found := p.cache_packages[guid]
	
	if found {//已经存在
		delete(p.cache_packages, guid)
	}
	
}
//注册缓存的 app package 到内存缓存
func (p *PackageMgm) registerCache(guid string, path string) {

	p.logger.Infof("register droplet cache ,guid:%v,path:%v",guid, path)
	
	p.lock.Lock()
	defer p.lock.Unlock()
	
	_, found := p.cache_packages[guid]
	
	if found {//已经存在
		delete(p.cache_packages, guid)
	}
	
	cachePackage := &Package{AppGuid:guid,CachePath:path,TimeOfLastUpdate:time.Now(),}
	p.cache_packages[guid] = cachePackage
}

//检测本地磁盘中缓存的文件是否存在,如果存在则删除该文件
func (p *PackageMgm) checkCacheFileAndRemove(path string) {

	if _, err := os.Stat(path); err == nil {
        p.logger.Infof("found file:%v, from cache disk and remove this",path)
        os.Remove(path)
    }
}

//根据guid返回 缓存文件在本地磁盘的路径
func (p *PackageMgm) getCachePath(guid string) string {
	
	rs := [] rune(guid)
	
	if len(rs) < 4{
		return ""
	}
	
	path := p.cache_base_dir+"/"+p.cache_directory+"/"+string(rs[0:2])+"/"+string(rs[2:4])
	//检测目录是否存在
	dir ,err := os.Stat(path)
	if err != nil || !dir.IsDir() {//目录不存在需要创建
		err := os.MkdirAll(path , 0777)
		if err != nil {
			p.logger.Errorf("mkdir path:%v fail,%v", path, err)
			return ""
		}
	}
	
	return path
}