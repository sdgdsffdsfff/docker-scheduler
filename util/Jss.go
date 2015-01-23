package util

import (
	"scheduler/config"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"os"
	"io/ioutil"
	"time"
	"strings"
	"net/http"
	"net"
	steno "github.com/cloudfoundry/gosteno"
)

//jss token 
type JssToken struct {
	SecretKey		string
	AccessKey		string
}

//生成调用jss 的GMT格式时间
func (t *JssToken) expires() string{
	duration := time.Duration(8) * time.Hour
	date := strings.Replace(time.Now().Add(-duration).Format(time.RFC1123Z),"+0800","GMT",-1)
	return date
}

//根据参数生成token 
func (t *JssToken) token(method string, md5 string, contentType string, expires string, resource string) string{
	
	h := hmac.New(sha1.New, []byte(t.SecretKey))
	param := []string{method,md5,contentType,expires,resource}
	io.WriteString(h, strings.Join(param,"\n"))
	sign := base64.StdEncoding.EncodeToString(h.Sum(nil))
	
	return "jingdong "+t.AccessKey+":"+sign
}


//云存储工具对象
type JssUtil struct {
	jssConfig    config.JssConfig
	droplet	   bool
	jssToken	   *JssToken
	logger       *steno.Logger
	//jss中key的后缀
	keySuffix	  string
	desc		  string
}

//创建一个JSS util对象
func NewJssUtil(c *config.Config, isDroplet bool) *JssUtil{
	
	var des string
	if isDroplet {
		des = "droplet"
	}else{
		des = "app package"
	}
	
	return &JssUtil{
		jssConfig: c.Jss,
		droplet:	isDroplet,
		jssToken:	&JssToken{
						SecretKey:c.Jss.SecretKey,
						AccessKey:c.Jss.AccessKey,
					},
		logger:   steno.NewLogger("cc_helper"),
		keySuffix:	"",
		desc:		des,
	}
}

//根据guid计算jss对应的 resource key
func (j *JssUtil) getResource(guid string) string{
	var resource string
	
	if j.droplet {
		resource = "/"+j.jssConfig.DropletBucket +"/"+guid+j.keySuffix
	}else{
		resource = "/"+j.jssConfig.AppPackageBucket +"/"+guid
	}
	
	return resource
}

//根据 jss 可以 返回请求jss的url
func (j *JssUtil) getRequestUrl(resource string) string {
	return j.jssConfig.Domain + resource
}

//上传文件到云存储
func (j *JssUtil) Upload(guid string ,filePath string) bool{

	if guid == "" {
		j.logger.Errorf("upload  %v from jss fail ,guid:%v is empty ",j.desc, guid)
		return false
	}
	
	start := time.Now()
	j.logger.Infof("upload %v from jss ,guid:%v",j.desc, guid)
	
	//解析上传文件
	reader , err := os.Open(filePath)
	defer reader.Close()
	
	if err !=nil {
		j.logger.Infof("upload %v from jss ,guid:%v, fail,%v",j.desc, guid, err)
		return false
	}
	finfo, err := reader.Stat()
	
	if err != nil {
		j.logger.Infof("upload %v from jss ,guid:%v, fail,%v",j.desc, guid, err)
		return false
	}
	
	fileSize := finfo.Size()
	j.logger.Infof("----------->size:%v,path:%v",fileSize, filePath)
	resource := j.getResource(guid)
	expires := j.jssToken.expires()
	token   := j.jssToken.token("PUT", "", "application/octet-stream", expires, resource)
	
	client := j.getHttpClient()//http.DefaultClient
	request,_ := http.NewRequest("PUT", j.getRequestUrl(resource), reader)
	request.Header.Set("authorization",token)
	request.Header.Set("date",expires)
	request.Header.Set("Content-Type","application/octet-stream")
	request.Header.Set("host",j.jssConfig.Host)
	request.Header.Set("accept","application/json")
	request.ContentLength = fileSize
	
	response, err:= client.Do(request)
	
	if err !=nil {
		j.logger.Errorf("upload %v to jss ,guid:%v fail:%v",j.desc, guid , err)
		return false
	}
	end := time.Now()
	
	if response.StatusCode == 200 {
		body, _ := ioutil.ReadAll(response.Body)
		bodyStr := string(body)
		j.logger.Infof("upload %v to jss ,guid:%v ,result:%v , success ,耗时:%v",j.desc, guid ,bodyStr, end.Sub(start))
		return true
	}else{
		body, _ := ioutil.ReadAll(response.Body)
		bodyStr := string(body)
		j.logger.Errorf("upload %v to jss ,guid:%v ,result:%v , fail ,耗时:%v",j.desc, guid ,bodyStr, end.Sub(start))
		return false
	}
}

//从jss中下载文件到指定路径
func (j *JssUtil) Download(guid string,filePath string , rw http.ResponseWriter) bool {

	if guid == "" {
		j.logger.Errorf("download %v from jss fail ,guid:%v is empty ",j.desc, guid)
		return false
	}
	
	start := time.Now()
	j.logger.Infof("download %v from jss ,guid:%v",j.desc, guid)
	
	resource := j.getResource(guid)

	
	expires := j.jssToken.expires()
	token   := j.jssToken.token("GET", "", "application/octet-stream", expires, resource)
	
	client := j.getHttpClient()//
	request,_ := http.NewRequest("GET", j.getRequestUrl(resource), nil)
	request.Header.Set("authorization",token)
	request.Header.Set("date",expires)
	request.Header.Set("Content-Type","application/octet-stream")
	request.Header.Set("host",j.jssConfig.Host)
	request.Header.Set("accept","application/json")
	
	response, err:= client.Do(request)
	
	if err !=nil {
		j.logger.Errorf("download %v to jss ,guid:%v fail:%v",j.desc, guid , err)
		return false
	}
	
	end := time.Now()
	
	if response.StatusCode == 200 {
		file, err := os.Create(filePath)
		
		defer file.Close()
		defer response.Body.Close()
		
		if err !=nil {
			j.logger.Infof("download %v from jss ,guid:%v ,result:%v , fail ,%v",j.desc, guid , err)
			return false
		}
		//io.Copy(file, response.Body)
//		file.Write(bodyByte)
//		rw.Write(bodyByte)
//		io.Copy(rw, response.Body)
		reader := response.Body
 		buf := make([]byte, 32*1024)
		for {
			nr, er := reader.Read(buf)
			if nr > 0 {
				nw, ew := rw.Write(buf[0:nr])
				file.Write(buf[0:nr])
				
				if ew != nil {
					err = ew
					break
				}
				if nr != nw {
					break
				}
			}
			if er == io.EOF {
				break
			}
			if er != nil {
				j.logger.Infof("download %v from jss ,guid:%v , fail",j.desc, guid , er)
				break
			}
		}
		j.logger.Infof("download %v from jss ,guid:%v , success ,耗时:%v",j.desc, guid , end.Sub(start))
		return true
	}else{
		body, _ := ioutil.ReadAll(response.Body)
		bodyStr := string(body)
		j.logger.Errorf("download %v from jss ,guid:%v ,result:%v , fail ,耗时:%v",j.desc, guid ,bodyStr, end.Sub(start))
		return false
	}
}

//根据guid从云存储删除对应的资源
func (j *JssUtil) Remove(guid string) bool{

	if guid == "" {
		j.logger.Errorf("remove %v from jss fail ,guid:%v is empty ",j.desc, guid)
		return false
	}
	
	start := time.Now()
	j.logger.Infof("remove %v from jss ,guid:%v",j.desc, guid)
	
	resource := j.getResource(guid)

	
	expires := j.jssToken.expires()
	token   := j.jssToken.token("DELETE", "", "application/octet-stream", expires, resource)
	
	client :=  j.getHttpClient()//http.DefaultClient
	request,_ := http.NewRequest("DELETE", j.getRequestUrl(resource), nil)
	request.Header.Set("authorization",token)
	request.Header.Set("date",expires)
	request.Header.Set("Content-Type","application/octet-stream")
	request.Header.Set("host",j.jssConfig.Host)
	request.Header.Set("accept","application/json")
	
	response, err:= client.Do(request)
	
	if err !=nil {
		j.logger.Errorf("remove %v to jss ,guid:%v fail:%v",j.desc, guid , err)
		return false
	}
	
	end := time.Now()
	
	if response.StatusCode == 200 {
		body, _ := ioutil.ReadAll(response.Body)
		bodyStr := string(body)
		j.logger.Infof("remove %v from jss ,guid:%v ,result:%v , success ,耗时:%v",j.desc, guid ,bodyStr, end.Sub(start))
		return true
	}else{
		body, _ := ioutil.ReadAll(response.Body)
		bodyStr := string(body)
		j.logger.Errorf("remove %v from jss ,guid:%v ,result:%v , fail ,耗时:%v",j.desc, guid ,bodyStr, end.Sub(start))
		return false
	}
}


//返回http client
func (j *JssUtil) getHttpClient() *http.Client{

	client := &http.Client{
                Transport: &http.Transport{
                        Dial: func(netw, addr string) (net.Conn, error) {
                                deadline := time.Now().Add(240 * time.Second)
                                c, err := net.DialTimeout(netw, addr, 5 * time.Second) //连接超时时间
                                if err != nil {
                                   return nil, err
                               		 }
                                c.SetDeadline(deadline)
                                return c, nil
                        },
                },
        }
        
    return client
}