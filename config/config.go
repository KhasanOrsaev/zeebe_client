package config

import (
	"github.com/go-errors/errors"
	"gopkg.in/yaml.v2"
)

type Configuration struct {
	Host     string `yaml:"host"`
	Port string `yaml:"port"`
	Workers int `yaml:"workers"`
}

func InitConfig(f []byte) (*Configuration,error) {
	var conf Configuration
	err := yaml.Unmarshal(f, &conf)
	if err != nil {
		return nil, errors.Wrap(err, -1)
	}
	return &conf, nil
}
