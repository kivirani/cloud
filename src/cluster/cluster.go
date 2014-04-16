package cluster

import (
	"encoding/xml"
	"fmt"
	"time"
	"os"
	"strconv"
	"bytes"
	"encoding/gob"
	"sync"
	zmq "github.com/pebbe/zmq4"
	"strings"

)

func New(id int, size_of_in_chan int,size_of_out_chan int,fnm string,delay_before_conn time.Duration) ServerMain {


	iid, _, port, serv, own_index := get_i_th_server_addr(id, fnm)



	in_chan := make(chan *Envelope, size_of_in_chan)
	out_chan := make(chan *Envelope, size_of_out_chan)
	new_chan_exit_send:=make(chan *bool)
	new_chan_exit_receive:=make(chan *bool)



	new_serv_sock, _ := zmq.NewSocket(zmq.PULL)

	addr := "tcp://*:" + strconv.Itoa(port)
	new_serv_sock.Bind(addr)

	new_peer_sock:=make([]*zmq.Socket,len(serv))



	time.Sleep(delay_before_conn)

	for k, _ := range serv {
		if k != own_index {

			n, _ := zmq.NewSocket(zmq.PUSH)

			str := "tcp://" + serv[k].Ip + ":" + strconv.Itoa(serv[k].Port)

			n.Connect(str)

			new_peer_sock[k] = n


		}

	}
	limit := len(serv)
	my_new_peer_sock := new_peer_sock[:limit]

	m := ServerMain{serv_sock: new_serv_sock, peer_sock: my_new_peer_sock, pid: iid, peers: serv, own_index_in_peers: own_index, in: in_chan, out: out_chan, Chan_exit_send:new_chan_exit_send,Chan_exit_receive:new_chan_exit_receive}

	return m
}




const (
	BROADCAST = -1
)

type Envelope struct {
	Pid int

	MsgId int

	Msg interface{}
}

type Myserver interface {
	Pid() int

	Peers() []Server

	Outbox() chan *Envelope

	Inbox() chan *Envelope

	//Sendtooutbox(string, int)
}

type ServerMain struct {
	serv_sock        *zmq.Socket
	peer_sock        []*zmq.Socket
	pid             int
	peers           []Server
	own_index_in_peers int
	in              chan *Envelope
	out             chan *Envelope
	Chan_exit_send  chan *bool
	Chan_exit_receive  chan *bool

}

func (server_main ServerMain) Pid() int {
	return server_main.pid
}
func (server_main ServerMain) Peers() []Server {
	return server_main.peers
}
func (server_main ServerMain) Outbox() chan *Envelope {
	return server_main.out
}
func (server_main ServerMain) Inbox() chan *Envelope {
	return server_main.in
}



//func (server_main ServerMain) Send(number_of_servers int,wg *sync.WaitGroup) {
func (server_main ServerMain) Send(wg *sync.WaitGroup) {
	for {

		select {
		case d := <-server_main.out :
			tmp := d.Pid
			d.Pid = server_main.Pid()

			var msg_buffer bytes.Buffer
			enc := gob.NewEncoder(&msg_buffer)
			err := enc.Encode(d)
			if err != nil {
				//log.Fatal("encode error:", err)
				fmt.Printf("\nError in encoding")
			}

			b:=msg_buffer.Bytes()



			for k, _ := range server_main.peers {

				if k != server_main.own_index_in_peers {
					if tmp==-1 {
						server_main.peer_sock[k].SendBytes(b, 0)
					} else {
						if tmp==server_main.peers[k].Id {


							server_main.peer_sock[k].SendBytes(b, 0)

						}
					}
				}
			}
			if strings.EqualFold(string(d.Msg.([]byte)), "FIN") {
				break
			}
		case <-server_main.Chan_exit_send :
			fmt.Printf("\nClosing sending in %d",server_main.pid)
			wg.Done()
			return
		}

	}
	wg.Done()
}

func (server_main ServerMain) Receive(number_of_servers int,wg *sync.WaitGroup) {


	var conn_arr = make([]int, number_of_servers)
	for k := 0; k < number_of_servers; k++ {
		conn_arr[k] = 0
	}

	for {

		server_main.serv_sock.SetRcvtimeo(-1)


		msg, err := server_main.serv_sock.RecvBytes(0)
		msg_buffer:=bytes.NewBuffer(msg)
		dec:=gob.NewDecoder(msg_buffer)
		var q Envelope
		err = dec.Decode(&q)
		if err != nil {
			//log.Fatal("decode error:", err)
			fmt.Printf("\nError in decoding")
		}


		server_main.Inbox() <- &q


		if strings.EqualFold(string(q.Msg.([]byte)), "FIN") {

			tmp:= q.Pid
			conn_arr[tmp] = 1
		}
		j := 0
		for j = 0; j < number_of_servers; j++ {
			if conn_arr[j] == 0 {
				break
			}
		}
		if j == number_of_servers {
			break
		}






	}
	wg.Done()
}



type Serverinfo struct {
	XMLName    Serverlist `xml:"serverinfo"`
	Serverlist Serverlist `xml:"serverlist"`
}
type Serverlist struct {
	Server []Server `xml:"server"`
}
type Server struct {
	Id   int `xml:"id"`
	Ip   string `xml:"ip"`
	Port int `xml:"port"`
}

/*max char of xmlfile are 10000 words*/
func get_i_th_server_addr(id int, fnm string) (int, string, int, []Server, int) {

	xmlFile, err := os.Open(fnm)
	if err != nil {
		fmt.Println("Error opening file:", err)

		return 0, "0", 0, nil, -1
	}
	defer xmlFile.Close()
	data := make([]byte, 10000)
	count, err := xmlFile.Read(data)
	if err != nil {
		fmt.Println("Can't read the data", count, err)
		return 0, "0", 0, nil, -1
	}
	var q Serverinfo
	xml.Unmarshal(data[:count], &q)
	checkError(err)

	for k, sobj := range q.Serverlist.Server {
		if sobj.Id==id {
			return q.Serverlist.Server[k].Id, q.Serverlist.Server[k].Ip, q.Serverlist.Server[k].Port, q.Serverlist.Server, k

		}
	}

	return 0, "0", 0, nil, -1
}
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}



