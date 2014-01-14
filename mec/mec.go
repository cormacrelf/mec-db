package main

import (
	"code.google.com/p/go-uuid/uuid"
	"flag"
	"fmt"
	"github.com/codegangsta/martini"
	"github.com/hashicorp/memberlist"
	"github.com/jmhodges/levigo"
	"net/http"
	"time"
	"github.com/cormacrelf/mec-db/peers"
	"io/ioutil"
)

// API
//
// GET /mec/key		-> 200 Value, 300 Multiple Responses
// GET

var m *martini.Martini
var db *levigo.DB

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
}

func main() {

	var name = flag.String("name", uuid.New(), "choose a server name")
	var port = flag.Int("port", 7946, "choose a port")
	var serve = flag.Int("serve", 3000, "choose an http server port")
	var remote = flag.Int("remote", 7946, "choose a remote")
	var join = flag.Bool("join", false, "choose a remote")
	flag.Parse()

	config := memberlist.DefaultLocalConfig()
	config.Name = *name
	config.BindAddr = "127.0.0.1"
	config.BindPort = *port
	pl := peers.Create(*port + 1)
	config.Events = pl
	config.LogOutput = ioutil.Discard
	list, err := memberlist.Create(config)
	if err != nil {
		panic("Failed to create memberlist: " + err.Error())
	}

	go func() {
		if *join {
			// Join an existing cluster by specifying at least one known member.
			for {
				_, err = list.Join([]string{fmt.Sprintf("127.0.0.1:%d", *remote)})
				if err != nil {
					// panic("Failed to join cluster: " + err.Error())
					// wait a second before retrying
					time.Sleep(1000 * time.Millisecond)
					continue
				} else {
					break
				}
			}
		}
	}()

	// m is assigned in shake()
	shake(*name)
	// http listens on 'serve' port
	err = http.ListenAndServe(fmt.Sprintf(":%d", *serve), m)
	if err != nil {
		fmt.Printf("failed to create server")
	} else {
		fmt.Printf("listening on port %d", *serve)
	}

	fmt.Printf("exiting?")
}
