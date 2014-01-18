package peers

import (
	"errors"
	"fmt"
	ml "github.com/hashicorp/memberlist"
	zmq "github.com/pebbe/zmq4"
	"math/rand"
	"time"
)

type PeerList struct {
	ml.EventDelegate
	Name    string
	router  *zmq.Socket
	dealers map[string]*zmq.Socket
	subs    map[string]*chan []string
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
	pl := &PeerList{
		Name:    name,
		router:  r,
		dealers: make(map[string]*zmq.Socket, 100),
		subs:    make(map[string]*chan []string, 100),
	}

	go pl.receive()

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

	(*p).dealers[node.Name] = sock
	// (*p).Message(node.Name, "HELLO")

}

// Delete a leaving node's interface
func (p *PeerList) NotifyLeave(node *ml.Node) {
	// (*p).dealers[node.Name].Close()
	fmt.Printf("LEFT:   %v, %v:%d\n", node.Name, node.Addr, node.Port)
	delete((*p).dealers, node.Name)
}

// Subscribes sender to a msgtype (eg WRITE): returns a chan through
// which all such messages will be forwarded.
func (p *PeerList) Subscribe(c chan []string, msgtype string) {
	(*p).subs[msgtype] = &c
}

// receive() receives messages on ROUTER in a loop
func (p PeerList) receive() {
	for {
		data, err := p.router.RecvMessage(0)
		if err != nil {
			fmt.Printf("router err %v\n", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// fmt.Printf("%v\n", data[1])

		// fmt.Printf("incoming: %v\n", data)
		// fmt.Printf("chans: %v\n", p.subs)

		msgtype := data[1]
		channel := p.subs[msgtype]

		// fmt.Printf("distributing to channel %s\n", data[1])
		*channel <- data
	}
}

func (p PeerList) Reply(msg ...string) {
	_, err := p.router.SendMessage(msg)
	if err != nil {
		fmt.Printf("router reply error %v\n", err)
	}
}

// Send one message to a named recipient
func (p PeerList) Message(recipient string, msg ...string) error {
	dest := p.dealers[recipient]
	_, err := dest.SendMessage(msg)
	if err != nil {
		fmt.Printf("dealer send error %v\n", err)
	}
	return err
}

// Send one message and await reply string
func (p PeerList) MessageExpectResponse(recipient string, timeout time.Duration, msg ...string) ([]string, error) {
	dest := p.dealers[recipient]
	_, err := dest.SendMessage(msg)
	if err != nil {
		fmt.Printf("dealer send error %v\n", err)
		return nil, errors.New(fmt.Sprintf("dealer send error %v\n", err))
	}

	response, err := dest.RecvMessage(0)
	if err != nil {
		return nil, errors.New("no response")
	}

	return response, nil
}

// Send multiple messages and await replies with a global timeout
func (p PeerList) MultiMessageExpectResponse(recipients []string, timeout time.Duration, msg ...string) map[string][]string {
	done := make(chan bool)
	acc := make(map[string][]string)
	for _, r := range recipients {
		r := r
		remote := p.dealers[r]
		remote.SendMessage(msg)
		go func(){
			msg, err := remote.RecvMessage(0)
			// fmt.Println("recvmsg: ", msg)
			if err == nil {
				acc[r] = msg
			}
			done <- true // trash value
		}()
	}

	for i := 0; i < len(recipients); i++ {
		<-done
	}

	return acc
}

func (p PeerList) RandomNodes() ([]string, int) {
	slice := make([]string, 0)
	for k, _ := range p.dealers {
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
	for i := 0; i < n; i++ {
		str, err := p.MessageExpectResponse(slice[i], 500*time.Millisecond, msg...)
		if len(str) < 1 {
			// so we don't get any index errors
			continue
		}
		if err != nil || str[0] != "GOOD" {
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

	responses := p.MultiMessageExpectResponse(slice[:n], 5000*time.Millisecond, msg...)

	// len(responses) <= n <= number of available clients
	return responses, len(responses)
}

func (p PeerList) Broadcast(msg ...string) int {
	acc := 0
	for k, _ := range p.dealers {
		err := p.Message(k, msg...)
		if err == nil {
			acc++
		}
	}
	return acc
}
