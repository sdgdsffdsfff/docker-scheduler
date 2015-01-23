package codec

import (
	steno "github.com/cloudfoundry/gosteno"
	"time"
	"strconv"
)

type StringCodec struct {
}

func NewStringCodec() steno.Codec {
	return new(StringCodec)
}

func (j *StringCodec) EncodeRecord(record *steno.Record) ([]byte, error) {
	
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	
	b := timeStr+" "+record.Source+" "+record.Level.Name+" File:"+record.File+" line:"+strconv.Itoa(record.Line)+" method:"+record.Method+" message:"+record.Message
	

	return []byte(b), nil
}