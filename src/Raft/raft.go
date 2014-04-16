package Raft
import (
	"encoding/xml"
	"fmt"
	"time"
	"os"
	"strconv"
	//"bytes"
	//"encoding/gob"
	"strings"
	"github.com/syndtr/goleveldb/leveldb"
	"kivirani/cluster"
	"math/rand"

)

var MAX_LOG_ENTRY_SIZE=50
var NO_OF_SERVERS=5

const (
	follower_phase=0
	leader_phase=1
	candidate_phase=2
)

const (
	vote_request_message=1
	vote_response_message=2
	append_entries_message=3
	append_entries_reponse_message=4
)

var raft Raft_Implementer

type RaftRPC int

type Int int
func (rpc *RaftRPC) Phase(args *Int,reply *int) error {
	*reply=raft.phase
	return nil
}
func (rpc *RaftRPC) Exit(args *Int,reply *int) error {

	//time.AfterFunc( 5* time.Second, func(){os.Exit(2);}
	raft.tmp_db.Close();
	raft.state_db.Close();
	raft.kvstore_db.Close();


	//raft.Close()
	time.AfterFunc( 5* time.Second, func(){os.Exit(2)})
	return nil

}


type Raft interface {
	Term()     int
	isLeader() bool
}

type Raft_Implementer struct {

	client_out_chan chan *string
	client_in_chan chan *string
	S cluster.ServerMain
	pid int
	ip string
	port int
	dir string
	timeout int
	serv []Raftserver
	own_index int


	curr_term int
	votedFor int
	log_Entries []Log_Entry
	fr int
	rr int
	commitIndex int
	lastApplied int
	lastLogIndex int
	lastLogTerm int
	sendMessagesFromIndexLastTime []int
	sendMessagesToIndexLastTime []int
	matchIndex []int
	nextIndex []int
	append_entry_received int


	heart_beat_frequency time.Duration
	chan_exit_follower chan *bool
	chan_exit_leader chan *bool
	chan_exit_candidate chan *bool
	chan_exit_inbox chan *bool
	chan_exit_Send_Append_Messages chan *bool
	chan_exit_raft_controller chan *bool
	chan_candidate_election_timeout chan *bool
	chan_follower_appendentries_election_timeout chan *bool


	exit_chan_candidate_election_timeout chan *bool
	exit_chan_follower_appendentries_election_timeout chan *bool

	exit_startReceivingClientRequests chan *bool

	curr_msg_id int
	phase int

	tmp_db *leveldb.DB
	state_db *leveldb.DB

	kvstore_db *leveldb.DB
}




func (r *Raft_Implementer) Phase() int {
	return r.phase
}

func NewRaft(myid int, size_of_in_chan int,size_of_out_chan int,delay_before_conn time.Duration,configfile string,parent_dir string) *Raft_Implementer {


	host_id,host_ip,host_port,host_dir,host_timeout,list_serv,host_own_index:=get_serverinfo(myid,configfile)

	new_chan_out_client:=make(chan *string)
	new_chan_in_client:=make(chan *string)

	new_chan_exit_follower:=make(chan *bool)
	new_chan_exit_leader:=make(chan *bool)
	new_chan_exit_candidate:=make(chan *bool)
	new_chan_exit_raft_controller:=make(chan *bool)
	new_chan_exit_inbox:=make(chan *bool)
	new_chan_exit_Send_Append_Messages:=make(chan *bool)
	new_exit_startReceivingClientRequests:=make(chan *bool)

	new_chan_candidate_election_timeout:=make(chan *bool)
	new_chan_follower_appendentries_election_timeout:=make(chan *bool)


	new_exit_chan_candidate_election_timeout:=make(chan *bool)
	new_exit_chan_follower_appendentries_election_timeout:=make(chan *bool)




	log_entry_arr:=make([]Log_Entry,MAX_LOG_ENTRY_SIZE)


	new_next_Index:=make([]int,NO_OF_SERVERS)
	new_sendMessagesFromIndexLastTime:=make([]int,NO_OF_SERVERS)
	new_match_Index:=make([]int,NO_OF_SERVERS)

	new_sendMessagesToIndexLastTime:=make([]int,NO_OF_SERVERS)

	for i:=0;i<NO_OF_SERVERS;i++ {

		new_next_Index[i]=0
		new_sendMessagesFromIndexLastTime[i]=(-1)
		new_sendMessagesToIndexLastTime[i]=(-1)
		new_match_Index[i]=(-1)


	}

	tmp_db_nm:="../raft/log/tmp"+strconv.Itoa(host_id)
	fmt.Println("nm:",tmp_db_nm)
	new_tmp_db, errr := leveldb.OpenFile(tmp_db_nm, nil)
	if errr!=nil {
		fmt.Println(" ",errr)
		panic("db error")
	}

	state_db_nm:="../raft/log/state"+strconv.Itoa(host_id)
	new_state_db, er := leveldb.OpenFile(state_db_nm, nil)
	if er!=nil {
		fmt.Println(" ",er)
		panic("db error")
	}


	kvstore_db_nm:="../kvstore/kvstore"+strconv.Itoa(host_id)
	new_kvstore_db, er := leveldb.OpenFile(kvstore_db_nm, nil)
	if er!=nil {
		fmt.Println(" ",er)
		panic("db error")
	}



	fnm := parent_dir+"/cluster/serverlist" + strconv.Itoa(host_id) + ".xml"

	my_server_main:= cluster.New(host_id, size_of_in_chan ,size_of_out_chan,fnm,delay_before_conn)

	raft=Raft_Implementer{client_out_chan:new_chan_out_client,client_in_chan:new_chan_in_client,S:my_server_main,pid:host_id,ip:host_ip,port:host_port,dir:host_dir,timeout:host_timeout,serv:list_serv,own_index:host_own_index,curr_term:0,heart_beat_frequency:3000*time.Millisecond,chan_exit_follower:new_chan_exit_follower,chan_exit_leader:new_chan_exit_leader,chan_exit_candidate:new_chan_exit_candidate,chan_exit_raft_controller:new_chan_exit_raft_controller,chan_exit_inbox:new_chan_exit_inbox,phase:0,log_Entries:log_entry_arr,commitIndex:-1,lastApplied:-1,lastLogIndex:-1,nextIndex:new_next_Index,matchIndex:new_match_Index,votedFor:-1,sendMessagesFromIndexLastTime:new_sendMessagesFromIndexLastTime,chan_exit_Send_Append_Messages:new_chan_exit_Send_Append_Messages,curr_msg_id:-1,lastLogTerm:-1,append_entry_received:-2,sendMessagesToIndexLastTime:new_sendMessagesToIndexLastTime,tmp_db:new_tmp_db,state_db:new_state_db,kvstore_db:new_kvstore_db,fr:-1,rr:-1,exit_startReceivingClientRequests:new_exit_startReceivingClientRequests,chan_candidate_election_timeout:new_chan_candidate_election_timeout,exit_chan_candidate_election_timeout:new_exit_chan_candidate_election_timeout,chan_follower_appendentries_election_timeout:new_chan_follower_appendentries_election_timeout,exit_chan_follower_appendentries_election_timeout:new_exit_chan_follower_appendentries_election_timeout}


	return &raft

}

func (r *Raft_Implementer) create_Bytes_of_Raft_msg_Envelope(d *Raft_Msg_Envelope) []byte {
	//s,_:=d.Actual_msg.([]byte)
	bb:=d.Actual_msg.(string)
	//fmt.Println("Original:",d)
	st:=strconv.Itoa(d.Type_of_raft_msg)+","+strconv.Itoa(d.Pid)+","+strconv.Itoa(d.Term)+","+bb
	//fmt.Println("Converted:",st)
	return []byte(st)
}

func (r *Raft_Implementer) get_Raft_msg_Envelope(b []byte) *Raft_Msg_Envelope {
	str:=string(b)

	typ:=-1
	pid:=-1
	term:=0
	msg:=""
	fmt.Sscanf(str,"%d,%d,%d,%s",&typ,&pid,&term,&msg)
	//fmt.Println("Str:",str)
	rme:=Raft_Msg_Envelope{Type_of_raft_msg:typ,Pid:pid,Term:term,Actual_msg:msg}
	return &rme
}

func (r *Raft_Implementer) RecomputeParameters() {
	k,t:=r.FindCommitIndex()
	fmt.Println("Last commited Index:",k," Term:",t)
	r.commitIndex=k
	r.lastApplied=k
	r.lastLogIndex=k
	r.lastLogTerm=t
	r.curr_term=t
	r.curr_msg_id=-1
	for i:=0;i<NO_OF_SERVERS;i++ {
		r.sendMessagesFromIndexLastTime[i]=k
		r.sendMessagesToIndexLastTime[i]=k
		r.matchIndex[i]=k
		r.nextIndex[i]=k+1
	}
	r.append_entry_received=k
}


func (r *Raft_Implementer) flushLogEntries() {
	fmt.Println("Last Applied:",r.lastApplied," CommitIndex:",r.commitIndex)
	if(r.lastApplied==r.commitIndex) {
		return
	}
	for i:=r.lastApplied+1;i<=r.commitIndex;i++ {
		v:=r.getFromTmpDB([]byte(strconv.Itoa(i)))
		r.setToStateDB([]byte(strconv.Itoa(i)),v)


	}
	r.lastApplied=r.commitIndex
	k:=r.findRelativeIndexof(r.lastApplied)
	if(k!=(-1)) {
		r.deleteToRelativeIndex(k)

	}
}


func (r *Raft_Implementer) addLogEntriesInTmpLogDB(logentries []Log_Entry) bool {
	for i:=0;i<len(logentries);i++ {
		key:=[]byte(strconv.Itoa(logentries[i].Logindex))
		value:=[]byte(strconv.Itoa(logentries[i].Term)+","+logentries[i].Key+","+logentries[i].Value)
		r.setToTmpDB(key,value)
	}
	return true
}

func (r *Raft_Implementer) addLogEntriesInStateLogDB(logentries []Log_Entry) bool {
	for i:=0;i<len(logentries);i++ {
		key:=[]byte(strconv.Itoa(logentries[i].Logindex))
		value:=[]byte(strconv.Itoa(logentries[i].Term)+","+logentries[i].Key+","+logentries[i].Value)
		r.setToStateDB(key,value)
	}
	return true
}
func (r *Raft_Implementer) displaylogentries() {
	if(r.isUnderFlow()) {
		fmt.Println("false")
		return
	}
	i:=0
	for i=r.fr;i!=r.rr; {
		fmt.Println(r.log_Entries[i])
		i=r.findNextRelativeIndexof(i)
	}
	fmt.Println(r.log_Entries[i])

}
func (r *Raft_Implementer) getLogEntriesFromTmpDBAndSetToStateDB(fromLogIndex int,toLogIndex int) bool {
	for i:=fromLogIndex;i<=toLogIndex;i++ {
		//ind:=r.isInLogEntriesLocalArray(i)
		ind:=r.findRelativeIndexof(i)
		if(ind==(-1)) {
			k:=[]byte(strconv.Itoa(i))
			v:=r.getFromTmpDB(k)
			r.setToStateDB(k,v)
		} else {
			k:=[]byte(strconv.Itoa(r.log_Entries[ind].Logindex))
			v:=[]byte(strconv.Itoa(r.log_Entries[ind].Term)+","+r.log_Entries[ind].Key+","+r.log_Entries[ind].Value)
			r.setToStateDB(k,v)
		}
	}
	return true
}

func (r *Raft_Implementer) ShowTmpDbContents() {
	iter := r.tmp_db.NewIterator(nil, nil)
	for iter.Next() {
		indexkey := iter.Key()
		v := iter.Value()
		term:=0
		key:=0
		value:=0
		fmt.Sscanf(string(v),"%d,%d,%d",&term,&key,&value)


		i:=""+string(indexkey)
		j,_:=strconv.Atoi(i)
		fmt.Println("j:",j," term:",term," key:",key," value:",value)

	}
	iter.Release()
	err := iter.Error()
	if err!=nil {
		fmt.Println(err)
	}

}
func (r *Raft_Implementer) ShowKVStoreDbContents() {
	iter := r.kvstore_db.NewIterator(nil, nil)
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		kk:=string(k)
		vv:=string(v)
		fmt.Println("key:",kk," Value:",vv)




	}
	iter.Release()
	err := iter.Error()
	if err!=nil {
		fmt.Println(err)
	}

}


func (r *Raft_Implementer) ShowStateDbContents() {
	iter := r.state_db.NewIterator(nil, nil)
	for iter.Next() {
		indexkey := iter.Key()
		v := iter.Value()
		term:=0
		key:=0
		value:=0
		fmt.Sscanf(string(v),"%d,%d,%d",&term,&key,&value)

		i:=string(indexkey)
		j,_:=strconv.Atoi(i)
		fmt.Println("j:",j," term:",term," key:",key," value:",value)

	}
	iter.Release()
	err := iter.Error()
	if err!=nil {
		fmt.Println(err)
	}

}

func (r *Raft_Implementer) FindCommitIndex() (int,int) {
	max:=-1
	maxterm:=-1
	iter := r.state_db.NewIterator(nil, nil)
	for iter.Next() {
		indexkey := iter.Key()
		v := iter.Value()
		term:=0
		key:=0
		value:=0
		fmt.Sscanf(string(v),"%d,%d,%d",&term,&key,&value)
		k,er:=strconv.Atoi(string(indexkey))

		if er!=nil {
			fmt.Println(er)
		}
		if k>max {
			max=k
			maxterm=term
		}

		//fmt.Println(key,value)

	}
	iter.Release()
	err := iter.Error()
	if err!=nil {
		fmt.Println(err)
	}
	return max,maxterm
}



func (r *Raft_Implementer) getLogEntriesFromLogIndex(fromLogIndex int,toLogIndex int) []Log_Entry {
	if( fromLogIndex>toLogIndex) {
		return nil
	}
	if( fromLogIndex==(-1) && toLogIndex==(-1) ) {
		//fmt.Println("ABCD")
		return nil
	}

	retlogentries:=make([]Log_Entry,toLogIndex-fromLogIndex+1)
	j:=0

	for i:=fromLogIndex;i<=toLogIndex;i++ {
		//ind:=r.isInLogEntriesLocalArray(i)
		ind:=r.findRelativeIndexof(i)
		if(ind==(-1)) {
			//retlogentries[j].Term,retlogentries[j].Key,retlogentries[j].Value=r.getAndDifferentiateFromTmpDB(i)
			term,key,value:=r.getAndDifferentiateFromTmpDB(i)
			retlogentries[j].Logindex=i
			retlogentries[j].Term=term
			retlogentries[j].Key=strconv.Itoa(key)
			retlogentries[j].Value=strconv.Itoa(value)

		} else {
			retlogentries[j].Logindex=r.log_Entries[ind].Logindex
			retlogentries[j].Term=r.log_Entries[ind].Term
			retlogentries[j].Key=r.log_Entries[ind].Key
			retlogentries[j].Value=r.log_Entries[ind].Value
		}
		j++
	}
	return retlogentries
}

func (r *Raft_Implementer) getAndDifferentiateFromTmpDB(logindex int) (int,int,int){
	k:=[]byte(strconv.Itoa(logindex))
	b:=r.getFromTmpDB(k)
	str:=string(b)
	var term int
	var key int
	var value int
	fmt.Sscanf(str,"%d,%d,%d",&term,&key,&value)
	return term,key,value
}


func (r *Raft_Implementer) getAndDifferentiateFromStateDB(logindex int) (int,int,int){
	k:=[]byte(strconv.Itoa(logindex))
	b:=r.getFromStateDB(k)
	str:=string(b)
	var term int
	var key int
	var value int
	fmt.Sscanf(str,"%d,%d,%d",&term,&key,&value)
	return term,key,value
}

func (r *Raft_Implementer) getFromTmpDB(key []byte) []byte {
	value, err := r.tmp_db.Get(key, nil)
	if err!=nil {
		return value
	}
	return value
}

func (r *Raft_Implementer) getFromStateDB(key []byte) []byte {
	value, err := r.state_db.Get(key, nil)
	if err!=nil {
		return value
	}
	return value
}

func (r *Raft_Implementer) setToTmpDB(key []byte,value []byte) {
	r.tmp_db.Put(key,value,nil)
}

func (r *Raft_Implementer) setToStateDB(key []byte,value []byte) {
	r.state_db.Put(key,value,nil)
	s:=string(value)
	t:=0
	k:=0
	v:=0
	fmt.Sscanf(s,"%d,%d,%d",&t,&k,&v)

	r.kvstore_db.Put([]byte(strconv.Itoa(k)),[]byte(strconv.Itoa(v)),nil)
}



func (r *Raft_Implementer) findRelativeIndexof(logIndex int) int {

	if(r.isUnderFlow()) {
		return -1
	} else {
		i:=r.fr
		for i=r.fr;(i<MAX_LOG_ENTRY_SIZE && i<=r.rr);i++ {
			if r.log_Entries[i].Logindex==logIndex {
				return i
			}
		}
		if i!=((r.rr+1)%MAX_LOG_ENTRY_SIZE) {
			for j:=0;j<=r.rr; j++ {
				if r.log_Entries[j].Logindex==logIndex {
					return j
				}
			}
		}
	}
	return -1
}

func (r *Raft_Implementer) isOverFlow() bool {
	if r.fr==((r.rr+1)%MAX_LOG_ENTRY_SIZE) {
		return true
	} else {
		return false
	}
	return false
}

func (r *Raft_Implementer) isUnderFlow() bool {
	if( r.fr==(-1) && r.rr==(-1) ) {
		return true
	}
	return false
}

func (r *Raft_Implementer) findNextRelativeIndexof(relindex int) int {
	maxbound:=MAX_LOG_ENTRY_SIZE
	ret:=(relindex+1)%maxbound
	return ret

}
func (r *Raft_Implementer) checkValidity(relIndex int) bool {
	if r.fr<=r.rr {
		if (relIndex<=r.rr && relIndex>=r.fr) {
			return true
		} else {
			return false
		}

	} else {
		if r.rr<=r.fr {
			if ( (relIndex>=0 && relIndex<=r.rr) || (relIndex>=r.fr && relIndex<=(MAX_LOG_ENTRY_SIZE-1) ) ) {
				return true
			} else {
				return false
			}
		} else {
			return false
		}
	}
	return false

}
func (r *Raft_Implementer) findPrevRelativeIndex(relindex int) int {
	maxbound:=MAX_LOG_ENTRY_SIZE
	ret:=(relindex-1)%maxbound
	if ret<0 {
		ret=maxbound+ret
	}
	return ret
}
func (r *Raft_Implementer) showQueueError() {
	panic("\nError -1 queue")
}

func (r *Raft_Implementer) dequeue() bool {
	if r.isUnderFlow() {
		return false
	} else {
		if r.fr==r.rr {
			r.fr=-1
			r.rr=-1
			return true
		}
		r.fr=r.findNextRelativeIndexof(r.fr)
		return true
	}
	return false
}
func (r *Raft_Implementer) pop() bool {
	if r.isUnderFlow() {
		return false
	} else {
		if r.fr==r.rr {
			r.fr=-1
			r.rr=-1
			return true
		}
		r.rr=r.findPrevRelativeIndex(r.rr)
		if r.rr==(-1) {
			r.showQueueError()
		}
		return true
	}
	return false
}

func (r *Raft_Implementer) deleteFromRelativeIndex(relativeIndex int) bool {
	if(r.isUnderFlow()) {
		return false
	} else {
		if !r.checkValidity(relativeIndex) {
			return false
		}
		if (r.rr<r.fr) {
			if ( (relativeIndex>=0 && relativeIndex<=r.rr) || (relativeIndex>=r.fr && relativeIndex<=(MAX_LOG_ENTRY_SIZE-1) ) ) {
				tmp:=r.findPrevRelativeIndex(relativeIndex)
				if r.fr==relativeIndex {
					r.fr=-1
					r.rr=-1
					return true
				}
				if r.checkValidity(tmp) {
					r.rr=tmp
				}
				if r.rr==(-1) {
					r.showQueueError()
				}
				return true
			} else {
				return false
			}
		} else {
			if(r.rr>r.fr) {
				if(relativeIndex>=r.fr && relativeIndex<=r.rr) {
					tmp:=r.findPrevRelativeIndex(relativeIndex)
					if r.fr==relativeIndex {
						r.fr=-1
						r.rr=-1
						return true
					}
					if r.checkValidity(tmp) {
						r.rr=tmp
					}
					if r.rr==(-1) {
						r.showQueueError()
					}
					return true
				} else {
					return false
				}
			} else {
				if(r.fr==r.rr && r.fr==relativeIndex) {
					r.fr=-1
					r.rr=-1
					return true
				} else {
					return false
				}
			}
		}

	}
	return false
}


func (r *Raft_Implementer) deleteToRelativeIndex(relativeIndex int) bool {
	if(r.isUnderFlow()) {
		return false
	} else {
		if !r.checkValidity(relativeIndex) {
			return false
		}
		if (r.rr<r.fr) {
			if ( (relativeIndex>=0 && relativeIndex<=r.rr) || (relativeIndex>=r.fr && relativeIndex<=(MAX_LOG_ENTRY_SIZE-1) ) ) {
				tmp:=r.findNextRelativeIndexof(relativeIndex)
				if r.rr==relativeIndex {
					r.fr=-1
					r.rr=-1
					return true
				}
				if r.checkValidity(tmp) {
					r.fr=tmp
				}
				if r.fr==(-1) {
					r.showQueueError()
				}
				return true
			} else {
				return false
			}
		} else {
			if(r.rr>r.fr) {
				if(relativeIndex>=r.fr && relativeIndex<=r.rr) {
					tmp:=r.findNextRelativeIndexof(relativeIndex)
					if r.rr==relativeIndex {
						r.fr=-1
						r.rr=-1
						return true
					}
					if r.checkValidity(tmp) {
						r.fr=tmp
					}
					if r.fr==(-1) {
						r.showQueueError()
					}
					return true
				} else {
					return false
				}
			} else {
				if(r.fr==r.rr && r.fr==relativeIndex) {
					return true
				} else {
					return false
				}
			}
		}

	}
	return false
}
func (r *Raft_Implementer) replaceAtRelativeIndex(relativeIndex int,term int,key string,value string) bool {
	if r.isUnderFlow() {
		return false
	}
	if(r.fr<=r.rr) {
		if (relativeIndex>=r.fr && relativeIndex<=r.rr) {
			r.log_Entries[relativeIndex].Term=term
			r.log_Entries[relativeIndex].Key=key
			r.log_Entries[relativeIndex].Value=value
		} else {
			return false
		}
	} else {
		if(r.fr>=r.rr) {
			if ( (relativeIndex>=0 && relativeIndex<=r.rr) || (relativeIndex>=r.fr && relativeIndex<=(MAX_LOG_ENTRY_SIZE-1) ) ) {
				r.log_Entries[relativeIndex].Term=term
				r.log_Entries[relativeIndex].Key=key
				r.log_Entries[relativeIndex].Value=value
			} else {
				return false
			}
		}
	}
	return false
}

//returns -1 if not found else returns relative index
func (r *Raft_Implementer) isInLogEntriesLocalArray(logindex int) int {
	if r.isUnderFlow() {
		return -1
	}
	maxbound:=MAX_LOG_ENTRY_SIZE
	firstLogIndex:=r.log_Entries[r.fr].Logindex
	lastLogIndex:=r.log_Entries[r.rr].Logindex
	distance:=logindex-firstLogIndex
	if (logindex>=firstLogIndex && logindex<=lastLogIndex) {
		if r.fr<=r.rr {
			return (r.fr+distance)%maxbound
		} else {
			if r.fr>=r.rr {
				return (r.fr+distance)%maxbound
			} else {
				return -1
			}
		}
	}
	return -1

}

func (r *Raft_Implementer) addToLogEntriesAndTmpDB(logindex int,term int,key []byte,value []byte) bool {
	if(r.push(logindex,term,key,value)) {
		k:=[]byte(strconv.Itoa(logindex))
		v:=[]byte(strconv.Itoa(term)+","+string(key)+","+string(value))
		r.setToTmpDB(k,v)
		return true
	} else {
		return false
	}
	return false
}

func (r *Raft_Implementer) push (logindex int,term int,key []byte,value []byte) bool {

	if r.isOverFlow() {
		r.dequeue()

	}
	if ( r.fr==(-1) && r.rr==(-1) ) {
		r.fr=0
		r.rr=r.findNextRelativeIndexof(r.rr)
		r.log_Entries[r.rr].Logindex=logindex
		r.log_Entries[r.rr].Term=term
		r.log_Entries[r.rr].Key=string(key)
		r.log_Entries[r.rr].Value=string(value)
	} else {
		r.rr=r.findNextRelativeIndexof(r.rr)
		r.log_Entries[r.rr].Logindex=logindex
		r.log_Entries[r.rr].Term=term
		r.log_Entries[r.rr].Key=string(key)
		r.log_Entries[r.rr].Value=string(value)
	}
	r.lastLogIndex=logindex
	r.lastLogTerm=term

	return true


}

func (r *Raft_Implementer) updateOwnParameters() {
	r.sendMessagesFromIndexLastTime[r.pid]=r.lastLogIndex
	r.sendMessagesToIndexLastTime[r.pid]=r.lastLogIndex
	//fmt.Println("\nBefore",r.pid," nextIndex:",r.nextIndex[r.pid]," matchindex:",r.matchIndex[r.pid])
	r.matchIndex[r.pid]=r.lastLogIndex
	r.nextIndex[r.pid]=r.lastLogIndex+1
	//fmt.Println("\nAfter ",r.pid,"nextIndex:",r.nextIndex[r.pid]," matchindex:",r.matchIndex[r.pid])
}

func (r *Raft_Implementer) updateOrAddLogEntries(logArr []Log_Entry) {
	//fmt.Println("Length of array ",len(logArr))
	if len(logArr)==0 {
		return
	}

	for i:=0;i<len(logArr);i++ {
		j:=r.findRelativeIndexof(logArr[i].Logindex)
		if j==(-1) && logArr[i].Logindex>r.lastLogIndex {

			r.addToLogEntriesAndTmpDB(logArr[i].Logindex,logArr[i].Term,[]byte(logArr[i].Key),[]byte(logArr[i].Value))


		} else {
			if(j==(-1) && logArr[i].Logindex<=r.lastLogIndex) {
				k:=[]byte(strconv.Itoa(logArr[i].Logindex))
				v:=[]byte(strconv.Itoa(logArr[i].Term)+","+logArr[i].Key+","+logArr[i].Value)
				r.setToTmpDB(k,v)
			} else {
				r.replaceAtRelativeIndex(j,logArr[i].Term,logArr[i].Key,logArr[i].Value)
			}
		}
	}

}


func (r *Raft_Implementer) waitForFollowerAppendEntriesElectionTimeout() {

	select {
	case <-time.After(time.Duration(r.timeout)*time.Millisecond) :
		b:=true
		r.chan_follower_appendentries_election_timeout<-&b

	case <-r.exit_chan_follower_appendentries_election_timeout :
		return
	}
}

func (r *Raft_Implementer) follower() int {
	r.votedFor=-1
	fmt.Println(r.pid," in follower with term:",r.curr_term)
	go r.waitForFollowerAppendEntriesElectionTimeout()
	for {

		select {
		case h:=<-r.S.Inbox() :

			msg, _ := h.Msg.([]byte)
			e:=r.get_Raft_msg_Envelope(msg)

			switch  e.Type_of_raft_msg {
			case vote_request_message:   //vote request

				var requestvote RequestVote
				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &requestvote)
				checkError(err)

				if requestvote.Term>r.curr_term {

					r.curr_term=requestvote.Term



				}

				if requestvote.Term<r.curr_term {
					str:=generateXmlRequestVoteResponse(r.curr_term,"false")

					d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}

					b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
					r.curr_msg_id++
					j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
					r.send_To_OutBox(&j)

					fmt.Println("\nSent Vote granted false to ",e.Pid)

				} else {

					if r.lastLogIndex<=requestvote.Lastlogindex && r.lastLogTerm<=requestvote.Lastlogterm  {

						bb:=false
						r.exit_chan_follower_appendentries_election_timeout<-&bb

						r.curr_term=requestvote.Term
						r.votedFor=requestvote.Candidateid
						str:=generateXmlRequestVoteResponse(r.curr_term,strconv.Itoa(r.commitIndex))
						fmt.Println("Sent votegrant follower:",r.commitIndex)
						d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}

						b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
						r.curr_msg_id++

						j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
						r.send_To_OutBox(&j)
						fmt.Println("Sent vote granted true to ",e.Pid)
						r.votedFor=e.Pid

						go r.waitForFollowerAppendEntriesElectionTimeout()

					} else {
						str:=generateXmlRequestVoteResponse(r.curr_term,"false")

						d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
						b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
						r.curr_msg_id++
						j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
						r.send_To_OutBox(&j)
						fmt.Println("\nSent Vote granted false to ",e.Pid)
					}
				}





			case vote_response_message:	  //vote grant  //ignore


				var requestvoteresponse RequestVoteResponse

				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &requestvoteresponse)
				checkError(err)


				if requestvoteresponse.Term>r.curr_term {
					r.curr_term=requestvoteresponse.Term

				}

				if(requestvoteresponse.Term==r.curr_term) {

					n,err:=strconv.Atoi(requestvoteresponse.Votegranted)
					fmt.Println("Got vote response from:",e.Pid,"with commitIndex:",requestvoteresponse.Votegranted)
					if err==nil {

						r.matchIndex[e.Pid]=n
						r.nextIndex[e.Pid]=n+1
						r.sendMessagesFromIndexLastTime[e.Pid]=n
						r.sendMessagesToIndexLastTime[e.Pid]=n
					} else {
						//got vote response false here

					}

				}
				//fmt.Printf("\nVote Response Rejected in Follower after receiving votegranted:pid:%d term:%d votegranted:%s\n",e.Pid,requestvoteresponse.Term,requestvoteresponse.Votegranted)


			case append_entries_message:	  //appendentries or heartbeat
				b:=false
				r.exit_chan_follower_appendentries_election_timeout<-&b


				var appendentries AppendEntries
				errr := xml.Unmarshal([]byte(e.Actual_msg.(string)), &appendentries)
				checkError(errr)
				if appendentries.Term>r.curr_term {
					r.curr_term=appendentries.Term
					//r.votedFor=e.Pid
					//return follower_phase
				}


				fmt.Printf("\nReceived appendentries in follower:term:%d prevlogindex:%d prevlogterm:%d logentries:%v",appendentries.Term,appendentries.Prevlogindex,appendentries.Prevlogterm,appendentries.Logentry)

				r.votedFor=e.Pid
				flg:=false

				if(appendentries.Prevlogindex!=-1) {
					prevlogentry:=r.getLogEntriesFromLogIndex(appendentries.Prevlogindex,appendentries.Prevlogindex)

					if prevlogentry!=nil {

						if prevlogentry[0].Term!=appendentries.Prevlogterm {
							//fmt.Println("I am here:",prevlogentry[0].Term," ",appendentries.Prevlogterm," ",appendentries.Prevlogindex," ",flg)
							flg=true
						}
					}
				}




				if appendentries.Term<r.curr_term  {



					str:=generateXmlAppendEntriesResponse(r.curr_term,"false")


					d:=Raft_Msg_Envelope{Type_of_raft_msg:append_entries_reponse_message,Pid:r.S.Pid(),Term:e.Term,Actual_msg:str}
					b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
					r.curr_msg_id++
					j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
					r.send_To_OutBox(&j)

					//fmt.Println("\nAppend Entries not successful..................",appendentries.Term,"  ",r.curr_term)



				} else {

					if flg==true {
						lastlogindex:=strconv.Itoa(r.lastLogIndex)
						//str:=generateXmlAppendEntriesResponse(r.curr_term,"false")
						str:=generateXmlAppendEntriesResponse(r.curr_term,lastlogindex)

						d:=Raft_Msg_Envelope{Type_of_raft_msg:append_entries_reponse_message,Pid:r.S.Pid(),Term:e.Term,Actual_msg:str}

						b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
						r.curr_msg_id++
						j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
						r.send_To_OutBox(&j)

						//fmt.Println("\nAppend Entries not successful..................",appendentries.Term,"  ",r.curr_term)


					} else {


						logentries:=r.getLogEntriesFromLogIndex(appendentries.Prevlogindex,appendentries.Prevlogindex)




						if logentries==nil && appendentries.Prevlogindex!=(-1) {

							lastlogindex:=strconv.Itoa(r.lastLogIndex)
							//str:=generateXmlAppendEntriesResponse(r.curr_term,"false")
							str:=generateXmlAppendEntriesResponse(r.curr_term,lastlogindex)

							d:=Raft_Msg_Envelope{Type_of_raft_msg:append_entries_reponse_message,Pid:r.S.Pid(),Term:e.Term,Actual_msg:str}
							b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
							r.curr_msg_id++
							j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
							r.send_To_OutBox(&j)

							//fmt.Println("\nAppend Entries not successful..................",appendentries.Term,"  ",r.curr_term)


						} else {


							r.curr_term=appendentries.Term
							r.votedFor=appendentries.Leaderid




							r.updateOrAddLogEntries(appendentries.Logentry)


							ln:=len(appendentries.Logentry)


							if (ln>0) {

								nextlogindex:=appendentries.Logentry[ln-1].Logindex+1

								in:=r.findRelativeIndexof(nextlogindex)
								if(in!=(-1) && nextlogindex<=r.lastLogIndex) {
									r.lastLogIndex=appendentries.Logentry[ln-1].Logindex
									r.deleteFromRelativeIndex(in)
									fmt.Println("Deleted")
									//os.Exit(1)

								}
							}



							str:=generateXmlAppendEntriesResponse(r.curr_term,"true")

							d:=Raft_Msg_Envelope{Type_of_raft_msg:append_entries_reponse_message,Pid:r.S.Pid(),Term:e.Term,Actual_msg:str}
							b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
							r.append_entry_received=e.Pid

							r.curr_msg_id++
							j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
							r.send_To_OutBox(&j)
							//fmt.Println("\nAppend Entries successful",len(appendentries.Logentry))


							if(appendentries.Leadercommit>r.commitIndex){
								fmt.Println("LeaderCommit:",appendentries.Leadercommit," CommitIndex:",r.commitIndex," LastLogIndex:",r.lastLogIndex)
								if(r.lastLogIndex>appendentries.Leadercommit) {
									r.commitIndex=appendentries.Leadercommit
									//update the storedb
									r.flushLogEntries()
								} else {
									r.commitIndex=r.lastLogIndex
									//update the storedb
									r.flushLogEntries()
								}
							}
						}
					}
				}

				go r.waitForFollowerAppendEntriesElectionTimeout()






			case append_entries_reponse_message:  //appendentries response//discard this message

				//discard this message
				var appendentriesresponse AppendEntriesResponse
				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &appendentriesresponse)
				checkError(err)
				//fmt.Printf("\nSuccess:%s\n",appendentriesresponse.Success)

				if appendentriesresponse.Term>r.curr_term {
					r.curr_term=appendentriesresponse.Term
					//r.votedFor=e.pid
					//return follower_phase
				}


			}
		case <-r.chan_follower_appendentries_election_timeout :
			r.votedFor=r.pid

			return candidate_phase

		case exit:=<-r.chan_Exit_Follower() :
			fmt.Printf("\nExiting from follower of %d:%v",r.pid,exit)
			return -1

		}


	}
	return -1
}


func (r *Raft_Implementer) sendAppendEntries() {
	for {
		select {
			//following is the heart beat frequency (append message frequency)
		case <-time.After(100*time.Millisecond) :

			for i:=0;i<NO_OF_SERVERS;i++ {

				if(i==r.pid) {
					continue
				}
				//var msg_buffer bytes.Buffer
				//enc := gob.NewEncoder(&msg_buffer)

				var str=""

				if( (r.nextIndex[i]==(r.lastLogIndex+1)) || (r.nextIndex[i]!=(r.matchIndex[i]+1))) {

					prevterm:=(-1)
					//r.commitIndex=(-1)
					//fmt.Println("Gott here1 ",r.sendMessagesFromIndexLastTime[i],r.sendMessagesToIndexLastTime[i])

					prevlogentry:=r.getLogEntriesFromLogIndex(r.nextIndex[i]-1,r.nextIndex[i]-1)
					if(prevlogentry==nil || (r.nextIndex[i]==-1) ) {
						prevterm=-1

					} else {
						prevterm=prevlogentry[0].Term
					}

					str=generateXmlAppendEntries(r.curr_term,r.pid,r.nextIndex[i]-1,prevterm,nil,r.commitIndex)
					//length:=0
					///rkfmt.Println("3 Sent appendentries with prevlogterm ",prevterm, " to ",i)


					r.sendMessagesFromIndexLastTime[i]=r.nextIndex[i]-1
					r.sendMessagesToIndexLastTime[i]=r.nextIndex[i]-1

				}else {
					if(r.nextIndex[i]<=r.lastLogIndex) {


						previndex:=r.sendMessagesToIndexLastTime[i]


						prevlogentry:=r.getLogEntriesFromLogIndex(r.sendMessagesToIndexLastTime[i],r.sendMessagesToIndexLastTime[i])


						flg:=false
						if r.sendMessagesToIndexLastTime[i]==-1 {
							flg=false
						} else {
							if prevlogentry==nil {
								flg=true
							}
						}


						if flg==true {
							prevterm:=(-1)

							//r.commitIndex=(-1)
							//fmt.Println("Gott here1 ",r.sendMessagesFromIndexLastTime[i],r.sendMessagesToIndexLastTime[i])
							str=generateXmlAppendEntries(r.curr_term,r.pid,previndex,prevterm,nil,r.commitIndex)
							///rkfmt.Println("2 Sent appendentries with prevterm -1 to",i)

						} else {

							//fmt.Println("Got here2 ",r.sendMessagesFromIndexLastTime[i],r.sendMessagesToIndexLastTime[i])
							prevterm:=0
							if(r.sendMessagesToIndexLastTime[i]==-1) {
								prevterm=-1
							} else {
								prevterm=prevlogentry[0].Term
							}

							length:=len(r.getLogEntriesFromLogIndex(r.nextIndex[i],r.lastLogIndex))

							if length>25 {
								length=25
							}
							r.sendMessagesFromIndexLastTime[i]=r.nextIndex[i]

							r.sendMessagesToIndexLastTime[i]=r.nextIndex[i]+length-1

							str=generateXmlAppendEntries(r.curr_term,r.pid,previndex,prevterm,r.getLogEntriesFromLogIndex(r.sendMessagesFromIndexLastTime[i],r.sendMessagesToIndexLastTime[i]),r.commitIndex)


							//length:=len(r.getLogEntriesFromLogIndex(r.nextIndex[i],r.lastLogIndex))

							///rkfmt.Println("1 Sent appendentries with prevterm ",prevterm," to ",i," from ",r.sendMessagesFromIndexLastTime[i]," to ",r.sendMessagesToIndexLastTime[i]," length ",length)

							//fmt.Println("\nSent nonempty logentry")

						}


					} else {


						prevterm:=(-1)
						//previndex:=r.sendMessagesToIndexLastTime[i]
						previndex:=(-1)

						length:=len(r.getLogEntriesFromLogIndex(0,r.lastLogIndex))
						if length>25 {
							length=25
						}
						r.sendMessagesFromIndexLastTime[i]=0

						r.sendMessagesToIndexLastTime[i]=length-1

						str=generateXmlAppendEntries(r.curr_term,r.pid,previndex,prevterm,r.getLogEntriesFromLogIndex(0,r.lastLogIndex),r.commitIndex)

						//length:=len(r.getLogEntriesFromLogIndex(0,r.lastLogIndex))

						///rkfmt.Println("0 Sent appendentries with prevterm ",prevterm," to ",i)
						//fmt.Println("Got here1 ",r.sendMessagesFromIndexLastTime[i],r.sendMessagesToIndexLastTime[i])
						//str=generateXmlAppendEntries(r.curr_term,r.pid,previndex,prevterm,nil,r.commitIndex)

					}

				}

				d:=Raft_Msg_Envelope{Type_of_raft_msg:append_entries_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
				/*err := enc.Encode(&d)
				if err != nil {

					fmt.Printf("\nError in encoding")
				}
				b:=msg_buffer.Bytes()*/
				b:=r.create_Bytes_of_Raft_msg_Envelope(&d)



				r.curr_msg_id++
				j:=r.createMsgToSend(i,r.curr_msg_id,b)
				r.send_To_OutBox(&j)
			}
			//fmt.Println("dfsfend:",r.nextIndex[0],r.lastLogIndex)


		case <-r.chan_exit_Send_Append_Messages :
			return
		}
	}
}
func (r *Raft_Implementer) startReceivingClientRequests() {
	key:=0
	val:=12
	for {
		select {
		case <-r.exit_startReceivingClientRequests :
			return

		case <-time.After(1000*time.Millisecond):
			r.addToLogEntriesAndTmpDB(r.lastLogIndex+1,r.curr_term,[]byte(strconv.Itoa(key)),[]byte(strconv.Itoa(val)))
			//fmt.Println("\nAdded a request")
			key++
			val++


			//time.Sleep(1000*time.Millisecond)
		}

	}
}

func (r *Raft_Implementer) initializeLeader() {
	for i:=0;i<NO_OF_SERVERS;i++ {
		r.matchIndex[i]=r.commitIndex
		r.nextIndex[i]=r.commitIndex+1
		r.sendMessagesFromIndexLastTime[i]=r.commitIndex
		r.sendMessagesToIndexLastTime[i]=r.commitIndex
		r.flushLogEntries()
	}
}
func (r *Raft_Implementer) leader() int {

	///rkfmt.Printf("\n%d in leader phase:",r.pid)

	r.initializeLeader()

	r.votedFor=r.pid
	go r.startReceivingClientRequests()
	//go r.Start_Handling_Request_From_Clients()
	//time.Sleep(100*time.Millisecond)
	go r.sendAppendEntries()
	for {

		select {
		case h:=<-r.S.Inbox() :
			msg, _ := h.Msg.([]byte)
			/*msg_buffer:=bytes.NewBuffer(msg)
			dec:=gob.NewDecoder(msg_buffer)
			var e Raft_Msg_Envelope
			errr:= dec.Decode(&e)
			if errr != nil {
				//log.Fatal("decode error:", err)
				fmt.Println("\n1:Error in decoding")
			}*/
			e:=r.get_Raft_msg_Envelope(msg)

			switch  e.Type_of_raft_msg {
			case vote_request_message:   //vote request
				///rkfmt.Println("Vote request got in leader")
				var requestvote RequestVote
				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &requestvote)
				checkError(err)


				if requestvote.Term>r.curr_term {
					r.curr_term=requestvote.Term
					//r.votedFor=e.pid
					b:=false
					r.chan_exit_Send_Append_Messages<-&b
					return follower_phase
				}

				if requestvote.Term<r.curr_term {
					///rkfmt.Println("Got here1??????")
					str:=generateXmlRequestVoteResponse(r.curr_term,"false")
					//var msg_buffer bytes.Buffer
					//enc := gob.NewEncoder(&msg_buffer)
					//vote granted
					d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
					/*err := enc.Encode(&d)
					if err != nil {

						fmt.Printf("\nError in encoding")
					}

					b:=msg_buffer.Bytes()*/
					b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
					r.curr_msg_id++
					j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
					r.send_To_OutBox(&j)

					//fmt.Println("\nSent Vote granted false")

				} else {
					///rkfmt.Println("Got here3??????")
					if r.lastLogIndex<requestvote.Lastlogindex && r.lastLogTerm<=requestvote.Lastlogterm {
						///rkfmt.Println("Got here4??????")
						r.curr_term=requestvote.Term
						r.votedFor=requestvote.Candidateid
						//fmt.Println("\nVoted to term ",r.curr_term)
						//var msg_buffer bytes.Buffer
						//enc := gob.NewEncoder(&msg_buffer)
						//str:=generateXmlRequestVoteResponse(r.curr_term,"true")
						str:=generateXmlRequestVoteResponse(r.curr_term,strconv.Itoa(r.commitIndex))
						///rkfmt.Println("Sent votegrant candidate:",r.commitIndex)
						//fmt.Println("\nSent Vote granted true")
						//vote granted
						d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
						/*err := enc.Encode(&d)
						if err != nil {
							fmt.Printf("\nError in encoding")
						}
						b:=msg_buffer.Bytes()*/
						b:=r.create_Bytes_of_Raft_msg_Envelope(&d)

						r.curr_msg_id++
						j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
						r.send_To_OutBox(&j)

						r.curr_term=requestvote.Term
						///rkfmt.Println("Changing to follower phase:2")
						bb:=false
						r.chan_exit_Send_Append_Messages<-&bb
						r.votedFor=e.Pid
						return follower_phase


					} else {
						///rkfmt.Println("Got here5??????")
						str:=generateXmlRequestVoteResponse(r.curr_term,"false")
						//var msg_buffer bytes.Buffer
						//enc := gob.NewEncoder(&msg_buffer)
						//vote granted
						d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}

						///rkfmt.Println("Got here51??????")
						/*err := enc.Encode(&d)
						if err != nil {

							fmt.Printf("\nError in encoding")
						}
						fmt.Println("Got here52??????")

						b:=msg_buffer.Bytes()*/
						b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
						r.curr_msg_id++
						j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
						r.send_To_OutBox(&j)

						///rkfmt.Println("\nSent Vote granted false")
					}

				}
			case vote_response_message:	  //vote grant //ignore vote grants here
				///rkfmt.Println("Vote response got in leader")
				var requestvoteresponse RequestVoteResponse
				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &requestvoteresponse)
				checkError(err)
				//fmt.Printf("\nReceved vote in leader:%v",requestvoteresponse)

				if(requestvoteresponse.Term>r.curr_term) {
					r.curr_term=requestvoteresponse.Term
					///rkfmt.Println("Changing to follower phase:1")
					b:=false
					r.chan_exit_Send_Append_Messages<-&b
					//r.votedFor=(-1)
					return follower_phase
				}

				if(requestvoteresponse.Term==r.curr_term) {

					n,err:=strconv.Atoi(requestvoteresponse.Votegranted)
					///rkfmt.Println("Got vote response true from:",e.Pid,"with commitIndex:",requestvoteresponse.Votegranted)
					if err==nil {

						r.matchIndex[e.Pid]=n
						r.nextIndex[e.Pid]=n+1
						r.sendMessagesFromIndexLastTime[e.Pid]=n
						r.sendMessagesToIndexLastTime[e.Pid]=n
					} else {
						//got vote response false here

					}

				}


			case append_entries_message:	  //appendentries or heartbeat	//from clients

				///rkfmt.Println("Got Appendentries in leader")
				var appendentries AppendEntries
				errr := xml.Unmarshal([]byte(e.Actual_msg.(string)), &appendentries)
				checkError(errr)


				if(appendentries.Term>r.curr_term) {
					///rkfmt.Println("Changing to follower phase:3")
					r.curr_term=appendentries.Term
					b:=false
					r.chan_exit_Send_Append_Messages<-&b
					//r.votedFor=e.Pid
					return follower_phase
				} else {
					if appendentries.Leadercommit>r.commitIndex {
						///rkfmt.Println("Changing to follower phase:4")
						r.curr_term=appendentries.Term
						b:=false
						r.chan_exit_Send_Append_Messages<-&b
						//r.votedFor=e.Pid
						return follower_phase
					}
				}

			case append_entries_reponse_message:   //appendentries reponse
				///rkfmt.Println("Got appendentries response in leader")
				var appendentriesresponse AppendEntriesResponse
				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &appendentriesresponse)
				checkError(err)
				if(appendentriesresponse.Term==r.curr_term) {
					///rkfmt.Println("AppendEntries Response:",appendentriesresponse," from:",e.Pid)
					if(strings.EqualFold(appendentriesresponse.Success,"true")) {
						//fmt.Println("Successful",r.fr," ",r.rr)


						i:=e.Pid
						r.matchIndex[i]=r.sendMessagesToIndexLastTime[i]




						r.nextIndex[i]=r.sendMessagesToIndexLastTime[i]+1




						r.commitIndex=r.calculateMajorityLeastMin()
						//for l:=0;l<len(r.matchIndex);l++ {
							///rkfmt.Println("MatchIndex:",l," ",r.matchIndex[l]," ",r.nextIndex[l])
						//}
						///rkfmt.Println("CalculateMajorityLeastMin",r.commitIndex)

						//store here in store_db

						r.flushLogEntries()

						//fmt.Println("lastApplied.............................................",r.lastApplied)




					} else {
						//fmt.Println("-vevalue....................................................")

						//r.nextIndex[e.Pid]--

						//fmt.Println("Successful",r.fr," ",r.rr)


						i:=e.Pid
						r.matchIndex[i],err=strconv.Atoi(appendentriesresponse.Success)
						if(err!=nil) {
							///rkfmt.Println("My Error here",appendentriesresponse.Success)
							os.Exit(1)
						}



						//fmt.Println("\n ",i," Before NextIndex"," ",r.nextIndex[i]," ",r.lastLogIndex)

						r.sendMessagesFromIndexLastTime[i]=r.matchIndex[i]
						r.sendMessagesToIndexLastTime[i]=r.matchIndex[i]
						r.nextIndex[i]=r.sendMessagesToIndexLastTime[i]+1


						//fmt.Println("\n ",i," After NextIndex"," ",r.nextIndex[i]," ",r.lastLogIndex)
						r.commitIndex=r.calculateMajorityLeastMin()
						//for l:=0;l<len(r.matchIndex);l++ {
						///rkfmt.Println("MatchIndex:",l," ",r.matchIndex[l]," ",r.nextIndex[l])
						//}
						//fmt.Println("CalculateMajorityLeastMin",r.commitIndex)

						//store here in store_db

						r.flushLogEntries()

						//fmt.Println("lastApplied.............................................",r.lastApplied)




					}
				} else {
					if(appendentriesresponse.Term>r.curr_term) {
						//r.curr_term=appendentriesresponse.Term
						///rkfmt.Println("Changing to follower phase:5")
						r.curr_term=appendentriesresponse.Term
						b:=false
						r.chan_exit_Send_Append_Messages<-&b
						//r.votedFor=(-1)
						return follower_phase
					}
				}




			}

		case exit:=<-r.chan_Exit_Leader() :
			fmt.Printf("Exiting from leader of %d:%v",r.pid,exit)
			return -1

		}

	}
	return -1
}



func (r *Raft_Implementer) calculateMajorityLeastMin() int {
	//fmt.Println("\nBefore",r.pid,"nextIndex:",r.nextIndex[r.pid]," matchindex:",r.matchIndex[r.pid])
	r.matchIndex[r.pid]=r.lastLogIndex

	r.nextIndex[r.pid]=r.lastLogIndex+1
	//fmt.Println("\nAfter",r.pid," nextIndex:",r.nextIndex[r.pid]," matchindex:",r.matchIndex[r.pid])
	r.sendMessagesFromIndexLastTime[r.pid]=r.lastLogIndex
	r.sendMessagesToIndexLastTime[r.pid]=r.lastLogIndex

	min_cnt:=make([]int,NO_OF_SERVERS)

	for i:=0;i<NO_OF_SERVERS;i++ {

		min_cnt[i]=0

		for j:=0;j<NO_OF_SERVERS;j++ {
			if r.matchIndex[i]<=r.matchIndex[j] {
				min_cnt[i]++
			}
		}
		//fmt.Println("Mincount:",min_cnt[i])
	}
	last_max:=(-1)
	for i:=0;i<NO_OF_SERVERS;i++ {
		if(min_cnt[i]>(NO_OF_SERVERS/2)) {
			if(last_max<r.matchIndex[i]) {
				last_max=r.matchIndex[i]
			}
		}
	}
	return last_max
}

func (r *Raft_Implementer) waitForCandidateElectionTimeout() {
	t:=rand.Intn(r.timeout/2)
	select {
		//case <-time.After(time.Duration(r.timeout)*time.Millisecond) :
	case <-time.After(time.Duration(t)*time.Millisecond) :
		b:=true
		r.chan_candidate_election_timeout<-&b
	case <-r.exit_chan_candidate_election_timeout :
		return
	}
}

func resetarray(r []int) []int {
	for i:=0;i<len(r);i++ {
		r[i]=-1
	}
	return  r
}
func isResponseAlreadyGot(id int,arr []int) bool {
	if arr[id]==1 {
		return true
	}
	return false

}
func no_of_responses_true_got(arr []int) int {
	cnt:=0
	for i:=0;i<len(arr);i++ {
		if arr[i]==1 {
			cnt++
		}
	}
	return cnt
	
}


func (r *Raft_Implementer) candidate() int {

	response_arr:=make([]int,NO_OF_SERVERS)
	response_arr=resetarray(response_arr)




	vote_granted:=1

	response_arr[r.pid]=1

	r.curr_term++
	r.votedFor=r.pid


	//term,candidateId,lastLogIndex,lastLogTerm
	str:=generateXmlRequestVote(r.curr_term,r.pid,r.lastLogIndex,r.lastLogTerm)
	//vote request
	d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_request_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
	b:=r.create_Bytes_of_Raft_msg_Envelope(&d)

	r.curr_msg_id++

	j:=make([]cluster.Envelope,NO_OF_SERVERS)
	for i:=0;i<NO_OF_SERVERS;i++ {
		j[i]=r.createMsgToSend(i,r.curr_msg_id,b)
		r.send_To_OutBox(&j[i])


	}

	go r.waitForCandidateElectionTimeout()
	//fmt.Printf("\nVote request send")

	for {


		select {


		case h:=<-r.S.Inbox() :

			msg, _ := h.Msg.([]byte)

			e:=r.get_Raft_msg_Envelope(msg)


			switch  e.Type_of_raft_msg {
			case vote_request_message:   //vote request
				var requestvote RequestVote
				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &requestvote)
				checkError(err)


				if requestvote.Term>r.curr_term {
					r.curr_term=requestvote.Term
					b:=true
					r.exit_chan_candidate_election_timeout<-&b
					return follower_phase
				}


				if requestvote.Term<r.curr_term {
					str:=generateXmlRequestVoteResponse(r.curr_term,"false")
					d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
					b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
					r.curr_msg_id++
					j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
					r.send_To_OutBox(&j)

					response_arr[e.Pid]=-1

				} else {
					if r.lastLogIndex<requestvote.Lastlogindex && r.lastLogTerm<=requestvote.Lastlogterm {


						r.curr_term=requestvote.Term
						r.votedFor=requestvote.Candidateid


						str:=generateXmlRequestVoteResponse(r.curr_term,strconv.Itoa(r.commitIndex))
						d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
						b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
						r.curr_msg_id++

						j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
						r.send_To_OutBox(&j)

						r.curr_term=requestvote.Term


						bb:=true
						r.exit_chan_candidate_election_timeout<-&bb
						return follower_phase
					} else {
						str:=generateXmlRequestVoteResponse(r.curr_term,"false")
						d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_response_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
						b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
						r.curr_msg_id++
						j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
						r.send_To_OutBox(&j)
						response_arr[e.Pid]=-1

					}
				}



			case vote_response_message:	  //vote grant

				var requestvoteresponse RequestVoteResponse

				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &requestvoteresponse)
				checkError(err)

				if requestvoteresponse.Term>r.curr_term {
					r.curr_term=requestvoteresponse.Term

					b:=true
					r.exit_chan_candidate_election_timeout<-&b
					//r.votedFor=e.pid
					return follower_phase
				}



				if(requestvoteresponse.Term==r.curr_term && strings.EqualFold(requestvoteresponse.Votegranted,"true")) {
					if !isResponseAlreadyGot(e.Pid,response_arr) {
						response_arr[e.Pid]=1
						vote_granted++

					}

				} else {
					if(requestvoteresponse.Term==r.curr_term) {

						n,err:=strconv.Atoi(requestvoteresponse.Votegranted)

						if err==nil {

							r.matchIndex[e.Pid]=n
							r.nextIndex[e.Pid]=n+1
							r.sendMessagesFromIndexLastTime[e.Pid]=n
							r.sendMessagesToIndexLastTime[e.Pid]=n

							if !isResponseAlreadyGot(e.Pid,response_arr) {
								vote_granted++
								response_arr[e.Pid]=1
							}
						} else {
							//got vote response false here



						}

					}
				}

				//if vote_granted >(len(r.S.Peers())/2) {
				if no_of_responses_true_got(response_arr)>(NO_OF_SERVERS/2) {
					//r.phase=leader_phase
					r.votedFor=r.pid

					b:=false
					r.exit_chan_candidate_election_timeout<-&b
					return leader_phase
				}
			case append_entries_message:	  //appendentries or heartbeat

				var appendentries AppendEntries

				errr := xml.Unmarshal([]byte(e.Actual_msg.(string)), &appendentries)
				checkError(errr)
	
				if(appendentries.Term>r.curr_term) {
					//r.votedFor=e.Pid
					r.curr_term=appendentries.Term
					b:=false
					r.exit_chan_candidate_election_timeout<-&b

					return follower_phase
					//fmt.Println("\nVoted to term",r.curr_term)
				}

				//fmt.Printf("\nReceived appendentries in candidate:term:%d prevlogindex:%d prevlogterm:%d",appendentries.Term,appendentries.Prevlogindex,appendentries.Prevlogterm)

				if( appendentries.Term<r.curr_term) {

					//term, success
					str:=generateXmlAppendEntriesResponse(r.curr_term,"false")


					d:=Raft_Msg_Envelope{Type_of_raft_msg:append_entries_reponse_message,Pid:r.S.Pid(),Term:e.Term,Actual_msg:str}
					b:=r.create_Bytes_of_Raft_msg_Envelope(&d)
					r.curr_msg_id++
					j:=r.createMsgToSend(e.Pid,r.curr_msg_id,b)
					r.send_To_OutBox(&j)

				}



			case append_entries_reponse_message:   //appendentries response //discard


				var appendentriesresponse AppendEntriesResponse

				err := xml.Unmarshal([]byte(e.Actual_msg.(string)), &appendentriesresponse)
				checkError(err)

				if(appendentriesresponse.Term>r.curr_term) {
					//r.votedFor=e.Pid
					r.curr_term=appendentriesresponse.Term
					b:=false
					r.exit_chan_candidate_election_timeout<-&b

					return follower_phase
					//fmt.Println("\nVoted to term",r.curr_term)
				}





			}
		case <-r.chan_candidate_election_timeout :

			str:=generateXmlRequestVote(r.curr_term,r.pid,r.lastLogIndex,r.lastLogTerm)

			d:=Raft_Msg_Envelope{Type_of_raft_msg:vote_request_message,Pid:r.S.Pid(),Term:r.curr_term,Actual_msg:str}
			b:=r.create_Bytes_of_Raft_msg_Envelope(&d)

			r.curr_msg_id++

			j:=make([]cluster.Envelope,NO_OF_SERVERS)
			for i:=0;i<NO_OF_SERVERS;i++ {
				j[i]=r.createMsgToSend(i,r.curr_msg_id,b)
				r.send_To_OutBox(&j[i])

			}
			vote_granted=1
			response_arr=resetarray(response_arr)

			response_arr[r.pid]=1
			go r.waitForCandidateElectionTimeout()





		case exit:=<-r.chan_Exit_Candidate() :
			fmt.Printf("\nExiting from candidate of %d :%v",r.pid,exit)
			return -1

		}


	}
	return -1
}


func (r * Raft_Implementer) Start_Handling_Request_From_Clients() {
	for {
		req:=<-r.Client_Outbox()
		//fmt.Println("Handler",r.votedFor)
		if(r.votedFor==(-1)) {
			continue
		}
		//fmt.Printf("\nRequest received %s",*req)
		k:=""
		v:=""
		fmt.Sscanf(*req,"set %s %s",&k,&v)


		if(!r.isLeader()) {
			return
			///rkfmt.Println("\nNot Added")

		} else {
			///rkfmt.Println("\nAdded.........................................................")

			r.addToLogEntriesAndTmpDB(r.lastLogIndex+1,r.curr_term,[]byte(k),[]byte(v))

		}
	}
}
func (r * Raft_Implementer) Client_Outbox() chan *string {
	return r.client_out_chan
}
func (r *Raft_Implementer) Client_Inbox() chan *string {
	return r.client_in_chan
}
func (r *Raft_Implementer) Term() int {

	return r.curr_term
}
func (r *Raft_Implementer) isLeader() bool {

	if r.phase==leader_phase {
		return true
	} else {
		return false
	}
	return false
}
type Raft_Msg_Envelope struct {
	Type_of_raft_msg int
	Pid int
	Term int
	Actual_msg interface {}
}
func (r *Raft_Implementer) createMsgToSend(pid int,msgId int,msgs []byte) cluster.Envelope {
	e:=cluster.Envelope{ Pid:pid, MsgId:msgId, Msg:msgs }
	return e
}
func (r *Raft_Implementer) chan_Exit_Inbox() chan *bool {
	return r.chan_exit_inbox
}
func (r *Raft_Implementer) chan_Exit_Follower() chan *bool {
	return r.chan_exit_follower
}
func (r *Raft_Implementer) chan_Exit_Leader() chan *bool {
	return r.chan_exit_leader
}
func (r *Raft_Implementer) chan_Exit_Candidate() chan *bool {
	return r.chan_exit_candidate
}
func (r *Raft_Implementer) send_To_OutBox(e *cluster.Envelope) {
	r.S.Outbox()<-e
}



func (r *Raft_Implementer) RaftController() {
	for {
		select {
		case <-r.chan_exit_raft_controller :
			//fmt.Printf("\nExiting from raft controller of %d",r.pid)
			return
		default :
			switch r.phase {

			case follower_phase:
				r.phase=r.follower()

			case leader_phase:
				r.phase=r.leader()


			case candidate_phase:
				r.phase=r.candidate()

			}

		}


	}
	//fmt.Printf("\nExiting")

}
func (r *Raft_Implementer) Close() {
	b:=false
	switch r.phase {
	case 0:
		r.phase=(-1)
		r.chan_Exit_Follower()<-&b
	case 1:
		r.phase=(-1)
		r.chan_Exit_Leader()<-&b
	case 2:
		r.phase=(-1)
		r.chan_Exit_Candidate()<-&b
	}
	r.chan_exit_raft_controller<-&b
	r.S.Chan_exit_send<-&b
	//r.S.chan_exit_receive<-&b
	r.chan_exit_inbox<-&b
	r.tmp_db.Close()
	r.state_db.Close()
	r.kvstore_db.Close()



}


type Raftserverinfo struct {
	XMLName    Raftserverlist `xml:"raftserverinfo"`
	Raftserverlist Raftserverlist `xml:"raftserverlist"`
}

type Raftserverlist struct {
	Raftserver []Raftserver `xml:"raftserver"`
}
type Raftserver struct {
	Id   int `xml:"id"`
	Ip   string `xml:"ip"`
	Port int `xml:"port"`
	Raft RaftStore `xml:"raft"`
}
type RaftStore struct {
	Dir string `xml:"dir"`
	Electiontimeout int `xml:"electiontimeout"`
}

/*max char of xmlfile are 100000 words*/
func get_serverinfo(id int, fnm string) (int, string, int, string, int,[]Raftserver, int) {


	xmlFile, err := os.Open(fnm)
	if err != nil {
		//fmt.Println("Error opening file:", err)
		return 0, "0", 0,"0",0,nil, -1
	}
	defer xmlFile.Close()
	data := make([]byte, 100000)
	count, err := xmlFile.Read(data)
	if err != nil {
		//fmt.Println("Can't read the data", count, err)
		return 0, "0", 0,"0",0,nil, -1
	}
	var q Raftserverinfo
	xml.Unmarshal(data[:count], &q)
	checkError(err)


	for k, sobj := range q.Raftserverlist.Raftserver {
		if sobj.Id==id {
			return q.Raftserverlist.Raftserver[k].Id, q.Raftserverlist.Raftserver[k].Ip, q.Raftserverlist.Raftserver[k].Port,q.Raftserverlist.Raftserver[k].Raft.Dir,q.Raftserverlist.Raftserver[k].Raft.Electiontimeout,q.Raftserverlist.Raftserver, k

		}
	}

	return 0, "0", 0,"0",0,nil, -1
}
func checkError(err error) {
	if err != nil {
		//fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

