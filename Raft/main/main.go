package main
import (

	"fmt"
	"time"
	"os"
	"strconv"
	"sync"
	"net/rpc"
	"net"
	"Raft"


)
var flg=0
const (
	MAX=200
)
var r *Raft.Raft_Implementer

func start_send_requests_client(r *Raft.Raft_Implementer) {

	for i:=0;i<MAX;i++ {
		req:="set "+strconv.Itoa(i)+" "+strconv.Itoa(MAX-i)
		if r.Phase()==1 {
			r.Client_Outbox()<-&req
		}

		time.Sleep(1000*time.Millisecond)
	}
}
/*func start_recv_responses_client(r *Raft.Raft_Implementer) {
	for i:=0;i<MAX;i++ {

		str:=<-r.Client_Inbox()
		actual:="commited set "+strconv.Itoa(i)+" "+strconv.Itoa(MAX-i)
		if strings.EqualFold(*str,actual) {
			fmt.Printf("\n%d 'th message commited successfully",i)
		} else {

			fmt.Printf("\n%d 'th message not committed",i)
		}
	}
}*/

func main() {






	id,_:=strconv.Atoi(os.Args[1])
	fmt.Println("Received here:",id," 2:",os.Args)
	number_of_servers:=5
	size_of_in_chan:=20
	size_of_out_chan:=20

	parent_dir:=""+os.Args[3]

	fnm:=parent_dir+"/Raft/configuration"+strconv.Itoa(id)+".xml"



	r=Raft.NewRaft(id, size_of_in_chan,size_of_out_chan,100*time.Millisecond,fnm,parent_dir)

	r.RecomputeParameters()






	wg:=new(sync.WaitGroup)
	wg.Add(2)
	go r.S.Send(wg)
	go r.S.Receive(number_of_servers,wg)
	//go r.Recv_From_Inbox()
	go r.RaftController()
	//go raft.Close()
	fmt.Println("\nIn main")

	//go r.Start_Handling_Request_From_Clients();

	//go start_send_requests_client(r)
	//go start_recv_responses_client(r)


	raft_rpc := new(Raft.RaftRPC)
	rpc.Register(raft_rpc)


	l, e := net.Listen("tcp", ":"+os.Args[2])


	if e != nil {
		//log.Fatal("listen error:", e)
		fmt.Printf("\nError in listening")
	}

	go func() {
		for {
			conn, errconn := l.Accept()
			if errconn!=nil {
				fmt.Printf("\nConnection error")
			}
			go rpc.ServeConn(conn)
		}
	}()


	wg.Wait()
	fmt.Println("Exiting")



}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

