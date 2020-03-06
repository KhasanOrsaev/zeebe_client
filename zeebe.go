package zeebe_client_git

import (
	"git.fin-dev.ru/dmp/zeebe_client.git/config"
	"github.com/go-errors/errors"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
)

type ZeebeClient struct {
	Configuration *config.Configuration
	Client zbc.Client
}

func NewClient() *ZeebeClient {
	return &ZeebeClient{}
}

var err error

func (client *ZeebeClient) SetConfig(f []byte) error {
	client.Configuration, err = config.InitConfig(f)
	return err
}

func (client *ZeebeClient) OpenConnection() error {
	if client.Configuration.Tls == "false" {
		client.Client, err = zbc.NewClient(&zbc.ClientConfig{
			GatewayAddress: client.Configuration.Host + ":" + client.Configuration.Port,
			UsePlaintextConnection: true,
		})
		if err!= nil {
			return errors.Wrap(err, -1)
		}
	} else {
		credsProvider, err := zbc.NewOAuthCredentialsProvider(&zbc.OAuthProviderConfig{
			ClientID: client.Configuration.User,
			ClientSecret: client.Configuration.Password,
			Audience: client.Configuration.Host,
		})
		if err!= nil {
			return errors.Wrap(err, -1)
		}

		client.Client, err = zbc.NewClient(&zbc.ClientConfig{
			GatewayAddress: client.Configuration.Host + ":" + client.Configuration.Port,
			CredentialsProvider: credsProvider,
			//UsePlaintextConnection: true,
		})

		if err!= nil {
			return errors.Wrap(err, -1)
		}
	}

	return nil
}

func (client *ZeebeClient) CloseConnection() error {
	err = client.Client.Close()
	if err != nil {
		return errors.Wrap(err,-1)
	}
	return nil
}
