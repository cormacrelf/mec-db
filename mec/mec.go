package main

import (
	"fmt"
	"github.com/codegangsta/martini"
	"github.com/cormacrelf/mec-db/peers"
	ml "github.com/hashicorp/memberlist"
	"github.com/jmhodges/levigo"
	"io/ioutil"
	"net/http"
	"time"
)

// API
//
// GET /mec/key		-> 200 Value, 300 Multiple Responses
// GET

var m *martini.Martini
var db *levigo.DB
var list *ml.Memberlist
var pl *peers.PeerList

func shake(name string) {
	m = martini.New()

	// Setup middleware
	m.Use(martini.Recovery())
	m.Use(martini.Logger())
	m.MapTo(jsonEncoder{}, (*Encoder)(nil))

	// Setup routes
	r := martini.NewRouter()
	r.Get(`/mec`, GetRoot)
	r.Get(`/mec/:key`, Get)
	r.Post(`/mec/:key/:value`, Post)
	r.Put(`/mec/:key/:value`, Put)
	r.Delete(`/mec/:key`, Delete)
	// Add the router action
	m.Action(r.Handle)

	// Inject database here so we get option parsing
	var dir = fmt.Sprintf("/Users/cormac/mec/%s", name)
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(dir, opts)
	if err != nil {
		panic("failed to create database")
	}
	m.Map(db)
	m.Map(pl)
}

func joinCluster(name string, port int, nodes []Node) {
	config := ml.DefaultLocalConfig()
	config.Name = name
	config.BindAddr = "127.0.0.1"
	config.BindPort = port
	pl = peers.Create(port + 1, name)
	ch := pl.Subscribe("HELLO")
	config.Events = pl
	config.LogOutput = ioutil.Discard
	list, err := ml.Create(config)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	hosts := make([]string, 100)
	for i, node := range nodes {
		hosts[i] = fmt.Sprintf("%s:%d", node.Host, node.Port)
	}

	go func() {
		// Join an existing cluster by specifying at least one known member.
		for {
			_, err := list.Join(hosts)
			if err != nil {
				// panic("Failed to join cluster: " + err.Error())
				// wait a second before retrying
				time.Sleep(1000 * time.Millisecond)
				continue
			} else {
				break
			}
		}
	}()

	// Our little fake module that receives HELLO msgs
	go func(ch *chan []string) {
		for {
			a := <-*ch
			fmt.Printf("RECEIVED: %v\n", a[1:])
		}
	}(ch)

}

func main() {
	config := GetConfig()

	joinCluster(config.Name, config.Port, config.Node)

	// m is assigned in shake()
	shake(config.Name)
	// http listens on 'serve' port
	err := http.ListenAndServe(fmt.Sprintf(":%d", config.HTTPPort), m)
	if err != nil {
		fmt.Printf("failed to create server")
	} else {
		fmt.Printf("listening on port %d", config.HTTPPort)
	}

	fmt.Printf("exiting?")
}
