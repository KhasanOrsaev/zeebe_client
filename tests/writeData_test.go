package tests

import (
	"fmt"
	zeebe_client_git "git.fin-dev.ru/dmp/zeebe_client.git"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"sync"
	"testing"
	"time"
)

func TestWriteData(t *testing.T)  {
	logger := logrus.WithField("test", "yes")
	outChannel := make(chan map[interface{}][]byte, 100)
	confirmChannel := make(chan interface{})
	crashChannel := make(chan map[uuid.UUID][]byte)
	f,err := ioutil.ReadFile("config_test.yaml")
	if err != nil {
		t.Error(err)
	}
	client := zeebe_client_git.NewClient()
	err = client.SetConfig(f)
	if err != nil {
		t.Error(err)
	}
	err = client.OpenConnection()
	if err != nil {
		t.Error(err)
	}
	defer client.CloseConnection()

	data1 := []byte(`{"zeebe":{"process_id" : "test_kafka_2","variables":{"name":"lalala", "key":"2", "lls": "dsw"}}}`)
	go func() {
		for i:= 0; i<1; i++ {
			outChannel <- map[interface{}][]byte{1:data1}
		}
		close(outChannel)
	}()

	ws := sync.WaitGroup{}
	ws.Add(3)
	go func() {
		t := time.Now()
		client.WriteData(outChannel, confirmChannel, crashChannel, logger)
		close(confirmChannel)
		close(crashChannel)
		fmt.Println("all instances sended by ", time.Since(t).Seconds(), " seconds")
		ws.Done()
	}()

	go func() {
		i := 0
		for b := range crashChannel {
			i++
			for i := range b {
				fmt.Println("crash uuid:", i.String())
			}
		}
		fmt.Println("errors: ", i)
		ws.Done()
	}()
	go func() {
		for b := range confirmChannel {
			fmt.Println("confirm:", b)
		}
		ws.Done()
	}()
	ws.Wait()
}

