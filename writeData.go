package zeebe_client_git

import (
	"context"
	"encoding/json"
	"github.com/go-errors/errors"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
)

type ZeebeRequest struct {
	Event Request `json:"zeebe"`
}

type Request struct {
	ProcessID       string                 `json:"process_id"`
	Variables       map[string]interface{} `json:"variables"`
	SessionLoggerID uuid.UUID              `json:"session_logger_id"`
}

func (client *ZeebeClient) WriteData(outChannel <-chan map[interface{}][]byte, confirmChannel chan<- interface{},
	crashChannel chan<- map[uuid.UUID][]byte, logger interface{}, sigchan chan os.Signal) {
	ws := sync.WaitGroup{}
	ws.Add(client.Configuration.Workers)
	for i := 0; i < client.Configuration.Workers; i++ {
		go func() {
			defer ws.Done()
			for b := range outChannel {
				for i, v := range b {
					// данные из очереди, json декодировка
					var event ZeebeRequest
					err := json.Unmarshal(v, &event)
					if err != nil {
						itemToCrash(confirmChannel, crashChannel, i, v, uuid.UUID{})
						logger.(*log.Entry).WithFields(log.Fields{
							"submodule":   "zeebe_client",
							"method":      "WriteData",
							"stack_trace": errors.Wrap(err, -1).ErrorStack(),
						}).Error(err.Error())
						continue
					}

					// validation
					if event.Event.ProcessID == "" {
						itemToCrash(confirmChannel, crashChannel, i, v, event.Event.SessionLoggerID)
						logger.(*log.Entry).WithFields(log.Fields{
							"submodule":         "zeebe_client",
							"method":            "WriteData",
							"session_logger_id": event.Event.SessionLoggerID,
						}).Error("process_id is empty")
						continue
					}

					// create request
					request, err := client.Client.NewCreateInstanceCommand().
						BPMNProcessId(event.Event.ProcessID).LatestVersion().VariablesFromMap(event.Event.Variables)
					if err != nil {
						itemToCrash(confirmChannel, crashChannel, i, v, uuid.UUID{})
						logger.(*log.Entry).WithFields(log.Fields{
							"submodule":   "zeebe_client",
							"method":      "WriteData",
							"stack_trace": errors.Wrap(err, -1).ErrorStack(),
						}).Error(err.Error())
						continue
					}
					// send to zeebe
					msg, err := request.Send(context.Background())
					if err != nil {
						itemToCrash(confirmChannel, crashChannel, i, v, uuid.UUID{})
						logger.(*log.Entry).WithFields(log.Fields{
							"submodule":   "zeebe_client",
							"method":      "WriteData",
							"stack_trace": errors.Wrap(err, -1).ErrorStack(),
						}).Error(err.Error())
						continue
					}
					logger.(*log.Entry).WithFields(log.Fields{
						"submodule":   "zeebe_client",
						"method":      "WriteData",
						"stack_trace": errors.Wrap(err, -1).ErrorStack(),
					}).Info(msg)
					confirmChannel <- i
				}
			}
		}()
	}

	ws.Wait()
}

func itemToCrash(confirmChannel chan<- interface{}, crashChannel chan<- map[uuid.UUID][]byte, i interface{}, v []byte, id uuid.UUID) {
	confirmChannel <- i
	crashChannel <- map[uuid.UUID][]byte{id: v}
}
