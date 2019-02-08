package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Release struct {
	Architectures []string `yaml:"archs"`
}

type DistroSource struct {
	Releases      map[string]Release `yaml:"releases"`
	GlanceOptions map[string]string  `yaml:"glance"`
}

type ImageSource struct {
	URLS          []string                `yaml:"urls,omitempty"`
	DistroSources map[string]DistroSource `yaml:"distros,omitempty"`
}

type Config struct {
	ImageSources ImageSource `yaml:"sources"`
}

func NewConfigFromFile(filename string) (*Config, error) {
	var config Config

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, err
}
