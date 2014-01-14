package peers

import (
	zmq "github.com/pebbe/zmq4"
	ml "github.com/hashicorp/memberlist"
	"fmt"
)

type PeerList struct {
	ml.EventDelegate
	router *zmq.Socket
	dealers map[string]*zmq.Socket
}

func Create(port int) *PeerList {
	r, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		panic("Can't create ROUTER socket")
	}
	addr := fmt.Sprintf("tcp://*:%d", port)
	err = r.Bind(addr)
	if err != nil {
		panic(fmt.Sprintf("Can't bind router on port %d", port))
	}
	pl := &PeerList {
		router: r,
		dealers: make(map[string]*zmq.Socket, 100),
	}

	go pl.receive()

	return pl
}

func (p *PeerList) NotifyJoin(node *ml.Node) {
	fmt.Printf("JOINED: %v, %v:%d\n", node.Name, node.Addr, node.Port)
	sock, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		panic("Can't create DEALER socket")
	}

	addr := fmt.Sprintf("tcp://%s:%d", node.Addr.String(), node.Port + 1)
	err = sock.Connect(addr)
	if err != nil {
		panic(fmt.Sprintf("Can't connect to router at %s", addr))
	}

	(*p).dealers[node.Name] = sock
	(*p).Message(node.Name, "HELLO")
}

func (p *PeerList) NotifyLeave(node *ml.Node) {
	delete((*p).dealers, node.Name)
}

func (p PeerList) receive() {
	for {
		data, err := p.router.RecvMessage(0)
		if err != nil {
			fmt.Printf("router err %v\n", err)
		}
		fmt.Printf("%v\n", data[1])
	}
}

func (p PeerList) Message(recipient, msg string) {
	dest := p.dealers[recipient]
	_, err := dest.SendMessage(msg)
	if err != nil {
		fmt.Printf("dealer error %v\n", err)
	}
}
