package config

import (
	"github.com/cloudfoundry-incubator/candiedyaml"
	"io/ioutil"
	
)

//nats 配置信息
type NatsConfig struct{

	Host string `yaml:"host"`
	Port uint16 `yaml:"port"`
	User string `yaml:"user"`
	Pass string `yaml:"pass"`
}

//初始化 natsConfig 的默认值
var defaultNatsConfig = NatsConfig{
	Host: "localhost",
	Port: 4222,
	User: "",
	Pass: "",
}

// 日志配置
type LoggingConfig struct {
	File  string `yaml:"file"`
	Level string `yaml:"level"`
}

// 日志配置默认值
var defaultLoggingConfig = LoggingConfig{
	File: "/export/home/jae/scheduler.log",
	Level: "debug",
}

//droplet 配置
type DropletConfig struct {
	
	//缓存文件根目录
	CacheBaseDir		string 			`yaml:"cache_base_dir"`
	//缓存文件 droplet 目录
	CacheDirecotry	string				`yaml:"cache_directory"`
	//缓存droplet 过期时间(默认10天)
	CacheTimeOut		int				`yaml:"cache_time_out_second"`
	//定时检测缓存时间(秒)(默认10小时执行一次)
	CacheInterval		int 			`yaml:"cache_interval_second"`
	//缓存磁盘最大空闲空间(使用百分比,超过该百分比将清空多余的缓存数据)
	DiskMaxUsedSpace	int				`yaml:"disk_max_used_space_percent"`
}

//默认的droplet 配置
var defaultDropletConfig = DropletConfig{
	CacheBaseDir:"/droplets",
	CacheDirecotry:"cc-droplets",
	CacheTimeOut:	60*60*24*10,
	CacheInterval:3600,
	DiskMaxUsedSpace:60,
}

//package 配置
type PackageConfig struct {
	//缓存文件根目录
	CacheBaseDir		string 			`yaml:"cache_base_dir"`
	//缓存文件 droplet 目录
	CacheDirecotry		string			`yaml:"cache_directory"`
	//缓存droplet 过期时间(默认10天)
	CacheTimeOut		int				`yaml:"cache_time_out_second"`
	//定时检测缓存时间(秒)(默认10小时执行一次)
	CacheInterval		int 			`yaml:"cache_interval_second"`
	//缓存磁盘最大空闲空间(使用百分比,超过该百分比将清空多余的缓存数据)
	DiskMaxUsedSpace	int				`yaml:"disk_max_used_space_percent"`
}

//默认的 package 配置
var defaultPackageConfig = DropletConfig{
	CacheBaseDir:"/droplets",
	CacheDirecotry:"cc-packages",
	CacheTimeOut:	60*60*24*10,
	CacheInterval:3600,
	DiskMaxUsedSpace:70,
}

//buildpack cache 配置
type BuildpackConfig struct {
	//缓存文件根目录
	CacheBaseDir		string 			`yaml:"cache_base_dir"`
	//缓存文件 droplet 目录
	CacheDirecotry		string			`yaml:"cache_directory"`
	//缓存droplet 过期时间(默认10天)
	CacheTimeOut		int				`yaml:"cache_time_out_second"`
	//缓存磁盘最大空闲空间(使用百分比,超过该百分比将清空多余的缓存数据)
	DiskMaxUsedSpace	int				`yaml:"disk_max_used_space_percent"`
}

var defaultBuildpackConfig = BuildpackConfig{
	CacheBaseDir:"/droplets",
	CacheDirecotry:"cc-buildpack",
	CacheTimeOut:	60*60*24*5,
	DiskMaxUsedSpace:70,
}

//云存储配置
type JssConfig struct {
	AccessKey			string			`yaml:"access_key"`
	SecretKey			string			`yaml:"secret_Key"`
	DropletBucket		string			`yaml:"droplet_bucket"`
	AppPackageBucket	string			`yaml:"app_package_bucket"`
	Host				string			`yaml:"host"`
	Domain				string			`yaml:"domain"`
	TimeOut				int				`yaml:"timeout_second"`
}

//云村村默认配置
var defaultJssConfig = JssConfig{
	AccessKey:		    "3e05c77d08a14045a0bd2ea307eb1ae9",
	SecretKey:		    "6212b165e5b54c3cb43d20295bf03e7aPIymHVqg",
	DropletBucket:		"jae-droplets",
	AppPackageBucket:	"jae-apppackage",
	Host:				"storage.jcloud.com",
	Domain:				"http://storage.jcloud.com",
	TimeOut:			60,
}

//cc 助手配置
type Config struct{
	
	Nats 			 NatsConfig 				`yaml:"nats"`
	Logging			 LoggingConfig              `yaml:"logging"`
	Droplet			 DropletConfig              `yaml:"droplet"`
	Package			 DropletConfig				`yaml:"package"`
	Jss				 JssConfig					`yaml:"jss"`
	Buildpack		 BuildpackConfig            `yaml:"buildpackcache"`
	
	//监控检测端口
	Port              string                      `yaml:port`
	//dea资源心跳检测超时时间
	DeaTimeoutSecond  int                         `yaml:dea_timeout_second`
	
}

var defaultConfig = Config{
	Nats:                   defaultNatsConfig,
	Logging:                defaultLoggingConfig,
	Port:                   "9091",
	DeaTimeoutSecond:       10,
	Droplet:				   defaultDropletConfig,
	Jss:					   defaultJssConfig,
	Buildpack:				   defaultBuildpackConfig,
	
}

func DefaultConfig() *Config{
	
	c := defaultConfig
	
	return &c
}

//解析配置文件,初始化配置对象
func (c *Config) Initialize(configYAML [] byte) error{

	return candiedyaml.Unmarshal(configYAML, &c)
}

//根据文件初始化配置对象
func InitConfigFromFile(path string) *Config{

	var c *Config = DefaultConfig()
	var e error
	
	b, e := ioutil.ReadFile(path)
	
	if e != nil {
		panic(e.Error())
	}
	
	e = c.Initialize(b)
	
	return c
}
