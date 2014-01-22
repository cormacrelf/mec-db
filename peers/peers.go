package peers

import (
	"fmt"
	ml "github.com/hashicorp/memberlist"
	zmq "github.com/pebbe/zmq4"
	"math/rand"
	"sync"
	"time"
)

var dealmutex = sync.Mutex{}
var dealers map[string]*zmq.Socket

var subs struct {
	sync.Mutex
	m map[chan []string]*handler
}

type handler struct {
	channel string
}

func (h *handler) want(ch string) bool {
	return h.channel == ch
}

type PeerList struct {
	ml.EventDelegate
	Name   string
	router *zmq.Socket
	rep1   *zmq.Socket
	rep2   *zmq.Socket
	// pseudo-methods for daemon
	send      chan []string
	expect    chan *Expecter
	sendmulti chan MultiSender
	reply     chan []string
	broadcast chan []string
}

// Create returns a new `*PeerList` initialised with its own
// ROUTER socket
func Create(port int, name string) *PeerList {
	r, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		panic("Can't create ROUTER socket")
	}
	addr := fmt.Sprintf("tcp://*:%d", port)
	err = r.Bind(addr)
	if err != nil {
		panic(fmt.Sprintf("Can't bind router on port %d", port))
	}

	rep, err := zmq.NewSocket(zmq.PAIR)
	if err != nil {
		panic("Can't create PAIR socket")
	}
	addr = "inproc://reply"
	err = rep.Bind(addr)
	if err != nil {
		panic(fmt.Sprintf("Can't bind PAIR on ", addr))
	}

	reper, err := zmq.NewSocket(zmq.PAIR)
	if err != nil {
		panic("Can't create PAIR socket")
	}
	addr = "inproc://reply"
	err = reper.Connect(addr)
	if err != nil {
		panic(fmt.Sprintf("Can't connect PAIR to ", addr))
	}

	pl := &PeerList{
		Name:      name,
		router:    r,
		rep1:      rep,
		rep2:      reper,
		send:      make(chan []string),
		expect:    make(chan *Expecter),
		sendmulti: make(chan MultiSender),
		reply:     make(chan []string),
		broadcast: make(chan []string),
	}

	dealers = make(map[string]*zmq.Socket, 100)

	go pl.runrouter()
	go pl.daemon(pl.send, pl.expect, pl.sendmulti, pl.broadcast)
	go pl.replydaemon(pl.reply)

	return pl
}

// Add an interface to any new node's ROUTER to our knowledge
func (p *PeerList) NotifyJoin(node *ml.Node) {
	if p.Name != node.Name {
		fmt.Printf("JOINED: %v, %v:%d\n", node.Name, node.Addr, node.Port)
	}
	sock, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		panic("Can't create DEALER socket")
	}

	addr := fmt.Sprintf("tcp://%s:%d", node.Addr.String(), node.Port+1)
	err = sock.Connect(addr)
	if err != nil {
		panic(fmt.Sprintf("Can't connect to router at %s", addr))
	}
	defer dealmutex.Unlock()
	dealmutex.Lock()
	dealers[node.Name] = sock
	// (*p).Message(node.Name, "HELLO")

}

// Delete a leaving node's interface
func (p *PeerList) NotifyLeave(node *ml.Node) {
	fmt.Printf("LEFT:   %v, %v:%d\n", node.Name, node.Addr, node.Port)
	defer dealmutex.Unlock()
	dealmutex.Lock()
	delete(dealers, node.Name)
}

// Subscribes sender to a msgtype (eg WRITE): returns a chan through
// which all such messages will be forwarded.
func (p *PeerList) Subscribe(c chan []string, msgtype string) {
	if c == nil {
		panic("Nil channel subscription.")
	}

	subs.Lock()
	defer subs.Unlock()

	h := subs.m[c]
	if h == nil {
		if subs.m == nil {
			subs.m = make(map[chan []string]*handler)
		}
		h = new(handler)
		subs.m[c] = h
	}

	h.channel = msgtype
}

// res := make(chan []string)
// res <- msg
// p.send <- res
// //
// e := <- send
// msg := <- e
// e <- response

type Expecter chan []string
type MultiSender struct {
	msg, recipients []string
	res             *chan map[string][]string
}

// daemon() isolates contact with zmq.Sockets to one goroutine
func (p *PeerList) daemon(send chan []string, expect chan *Expecter, sendmulti chan MultiSender, broadcast chan []string) {
	for {
		select {
		case e := <-send:
			// format: [dest msg...]
			recipient := e[0]
			dest := dealers[recipient]
			_, err := dest.SendMessage(e[1:])
			if err != nil {
				fmt.Printf("dealer send error %v\n", err)
			}
		case e := <-expect:
			// format: [dest msg...]
			msg := <-*e
			recipient := msg[0]
			dest := dealers[recipient]
			_, err := dest.SendMessage(msg[1:])
			if err != nil {
				fmt.Printf("dealer send error %v\n", err)
			}
			res, err := dest.RecvMessage(0)
			*e <- res
		case args := <-sendmulti:
			// format: [dest: msg, dest2: msg2]
			acc := make(map[string][]string, len(args.recipients))
			for _, r := range args.recipients {
				remote := dealers[r]
				remote.SendMessage(args.msg)
			}
			for _, r := range args.recipients {
				remote := dealers[r]
				msg, err := remote.RecvMessage(0)
				if err == nil {
					acc[r] = msg
				}
			}
			*args.res <- acc
		case msg := <-broadcast:
			for k, _ := range dealers {
				dest := dealers[k]
				_, err := dest.SendMessage(msg)
				if err != nil {
					fmt.Printf("dealer send error %v\n", err)
				}
			}
		}
	}
}

// wrap the router communication PAIR in a familiar chan
func (p *PeerList) replydaemon(reply chan []string) {
	for {
		select {
		case msg := <-reply:
			// format: [router_data msg]
			_, err := p.rep1.SendMessage(msg)
			if err != nil {
				fmt.Printf("router reply error %v\n", err)
			}
		}
	}
}

// isolate router usage to one goroutine
func (p PeerList) runrouter() {
	poller := zmq.NewPoller()
	poller.Add(p.router, zmq.POLLIN)
	poller.Add(p.rep2, zmq.POLLIN)
	//  Process messages from both sockets
	for {
		sockets, _ := poller.Poll(-1)
		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case p.router:
				data, err := p.router.RecvMessage(0)
				if err != nil {
					fmt.Printf("router err %v\n", err)
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if len(data) <= 1 {
					continue
				}
				msgtype := data[1]
				subs.Lock()

				for c, h := range subs.m {
					if h.want(msgtype) {
						c <- data
					}
				}

				subs.Unlock()
			case p.rep2:
				msg, err := s.RecvMessage(0)
				if err != nil {
					fmt.Println("couldn't receive")
				}
				_, err = p.router.SendMessage(msg)
				if err != nil {
					fmt.Println("couldn't send\n")
				}
			}
		}
	}
}

func (p PeerList) Reply(msg ...string) {
	p.reply <- msg
}

// Send one message to a named recipient
func (p PeerList) Message(recipient string, msg ...string) error {
	p.send <- append([]string{recipient}, msg...)
	return nil
}

// Send one message and await reply string
func (p PeerList) MessageExpectResponse(recipient string, timeout time.Duration, msg ...string) ([]string, error) {
	res := make(Expecter)
	res <- append([]string{recipient}, msg...)
	p.expect <- &res
	str := <-res
	return str, nil
}

// Send multiple messages and await replies with a global timeout
func (p PeerList) MultiMessageExpectResponse(recipients []string, timeout time.Duration, msg ...string) map[string][]string {
	res := make(chan map[string][]string)
	p.sendmulti <- MultiSender{msg, recipients, &res}
	return <-res
}

func (p PeerList) RandomNodes() ([]string, int) {
	defer dealmutex.Unlock()
	dealmutex.Lock()
	slice := make([]string, 0)
	for k, _ := range dealers {
		slice = append(slice, k)
	}
	for i := range slice {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}

	return slice, len(slice)
}

// Send msg to n random nodes from cluster (for a read/write op)
// Returns number of messages sent (e.g. if n < available members)
func (p PeerList) SendRandom(n int, msg ...string) int {
	slice, t := p.RandomNodes()
	if n > t {
		n = t
	}
	for i := 0; i < n; i++ {
		p.Message(slice[i], msg...)
	}

	return n
}

// Verify we have `n` GOOD responses to a message
// The alternative is an error or a "FAIL"
func (p PeerList) VerifyRandom(n int, msg ...string) int {
	slice, t := p.RandomNodes()
	if n > t {
		n = t
	}

	acc := 0
	res := make(Expecter)
	for i := 0; i < n; i++ {
		p.expect <- &res
		res <- append([]string{slice[i]}, msg...)
		str := <-res
		if len(str) < 1 {
			// so we don't get any index errors
			continue
		}
		if str[0] != "GOOD" {
			continue
		}
		acc += 1
	}

	// acc <= n <= number of nodes we could find
	// ideally acc == n
	return acc
}

// Returns all replies from N random nodes to caller
func (p PeerList) RandomResponses(n int, msg ...string) (map[string][]string, int) {
	slice, t := p.RandomNodes()
	if n > t {
		n = t
	}

	res := make(chan map[string][]string)
	p.sendmulti <- MultiSender{msg, slice[:n], &res}
	responses := <-res

	// len(responses) <= n <= number of available clients
	return responses, len(responses)
}

func (p PeerList) Broadcast(msg ...string) int {
	p.broadcast <- msg
	return 0
}
