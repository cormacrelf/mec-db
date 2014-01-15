package main

import (
	"code.google.com/p/go-uuid/uuid"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
)

type Node struct {
	Host string
	Port int
}

type Config struct {
	Name     string
	Port     int
	HTTPPort int
	Node    []Node
	Root     string // Database directory
}

func GetConfig() Config {
	var conf Config

	usr, _ := user.Current()
	dir := usr.HomeDir

	fallback := fmt.Sprintf("%s/mec/config.conf", dir)
	var loc = flag.String("config", fallback, "specify a config file")
	flag.Parse()

	location, _ := filepath.Abs(*loc)
	tomlData, err := ioutil.ReadFile(location)
	if err != nil {
		fmt.Printf("couln't find config file %s", *loc)
		os.Exit(1)
	}

	md, err := toml.Decode(string(tomlData), &conf)
	if err != nil {
		fmt.Printf("couldn't decode config file: %v", err)
		os.Exit(1)
	}

	if md.IsDefined("name") == false {
		conf.Name = uuid.New()
	}
	if md.IsDefined("port") == false {
		conf.Port = 7000
	}
	if md.IsDefined("httpport") == false {
		conf.HTTPPort = 3000
	}
	if md.IsDefined("root") == false {
		usr, _ := user.Current()
		conf.Root = fmt.Sprintf("%s/mec/%s", usr.HomeDir, conf.Name)
	}

	return conf
}

