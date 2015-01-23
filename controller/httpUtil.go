package controller

import (
	"net/http"
	"encoding/json"
	"errors"
	"io"
)

type RequestForm struct {
	FormData 		map[string]interface{}
}

func (f *RequestForm) getParam(key string) string{
	
	if f.FormData == nil {
		return ""
	}
	
	v ,found := f.FormData[key]
	if !found {
		return ""	
	}
	val , ok := v.(string)
	if ok {
		return val	
	}
	return ""
}


//parse body 
func Decode(body io.Reader) (RequestForm, error) {
	
	m := make(map[string]interface{} )
	
	if err := json.NewDecoder(body).Decode(&m); err != nil {
		return 	RequestForm{},errors.New("Decode Reques form fail."+err.Error() )
	}
	
	result := RequestForm{
		FormData: m,
	}
	return result, nil
}

//  响应http请求
func writeJson(data []byte, rw http.ResponseWriter) {
	rw.Header().Set("Content-Type","application/json")
	rw.Write(data)
}

func writeStr(data string, rw http.ResponseWriter ) {
	rw.Header().Set("Content-Type","application/x-drw")
	rw.Write([]byte(data))
}


//将对象转换成json格式
func encodeJson(v interface{}) ([] byte, error){
	
	r,err := json.Marshal(v)
	
	if err != nil {
		return nil,err
	}
	
	return r,nil
}