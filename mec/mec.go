package main

import (
	"fmt"
	"github.com/codegangsta/martini"
	"github.com/cormacrelf/mec-db/peers"
	"github.com/cormacrelf/mec-db/store"
	ml "github.com/hashicorp/memberlist"
	"github.com/jmhodges/levigo"
	"io/ioutil"
	"net/http"
	"time"
	"os"
	"os/signal"
)

// API
//
// GET /mec/key		-> 200 Value, 300 Multiple Responses
// GET

var m *martini.Martini
var db *levigo.DB
var list *ml.Memberlist
var pl *peers.PeerList

func shake(name string, root string) {
	m = martini.New()

	// Setup middleware
	m.Use(martini.Recovery())
	m.Use(martini.Logger())
    m.Use(MapEncoder)

	// Setup routes
	r := martini.NewRouter()
	r.Get(`/mec`, GetRoot)
	r.Get(`/mec/:key`, Get)
	r.Post(`/mec/:key`, Post)
	r.Put(`/mec/:key/:value`, Put)
	r.Delete(`/mec/:key`, Delete)
	// Add the router action
	m.Action(r.Handle)

	// Inject database here so we get option parsing
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(root, opts)
	if err != nil {
		panic("failed to create database")
	}

	s := store.Create(db, pl)

	m.Map(db)
	m.Map(pl)
	m.Map(s)

}

func joinCluster(name string, port int, nodes []Node) {
	config := ml.DefaultLocalConfig()
	config.Name = name
	config.BindAddr = "127.0.0.1"
	config.BindPort = port
	pl = peers.Create(port+1, name)
	ch := make(chan []string, 3)
	pl.Subscribe(ch, "HELLO")
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
	go func() {
		for a := range ch{
			fmt.Printf("RECEIVED: %v\n", a[1:])
		}
	}()

}

func shutdown() {
		db.Close()
		list.Leave(500 * time.Millisecond)
		list.Shutdown()
}

func main() {
	config := GetConfig()

	joinCluster(config.Name, config.Port, config.Node)

	// m is assigned in shake()
	shake(config.Name, config.Root)

	// Restart cluster on interrupt
	go func() {
		c1 := make(chan os.Signal, 1)
		signal.Notify(c1, os.Interrupt)
		c2 := make(chan []string, 1)
		pl.Subscribe(c2, "RESTART")
		for {
			select {
			case _ = <-c1:
				// sig is a ^C, handle it
				fmt.Println("\nrestarting...")
				pl.Broadcast("RESTART")
				// Wait for the RESTART message to get to everyone
				time.Sleep(200 * time.Millisecond)
				os.Exit(2)
			case _ = <-c2:
				os.Exit(2)
			}
		}
	}()


	// http listens on 'serve' port
	err := http.ListenAndServe(fmt.Sprintf(":%d", config.HTTPPort), m)
	if err != nil {
		fmt.Printf("failed to create server: %v", err)
	}
}
