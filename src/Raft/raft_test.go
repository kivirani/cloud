package Raft

import (
	"testing"
	"strconv"
	"sync"
	"time"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"fmt"
	"log"


	"github.com/syndtr/goleveldb/leveldb"

)

var MAX_NO_OF_SERVERS=5





func getPhase(rpc_port int) int {
	client, err := rpc.Dial("tcp", "localhost" + ":"+strconv.Itoa(rpc_port))

	reply:=-1
	if err != nil {

		return -1
	} else {


		I:=10

		errr := client.Call("RaftRPC.Phase", &I, &reply)
		if errr != nil {

			//fmt.Println("\nError comes")
			return -1
		}
		client.Close()
	}
	return reply
}


func isCandidate(phase int) bool {
	if phase==candidate_phase {
		return true
	}
	return false
}

func isLeader(phase int) bool {
	if phase==leader_phase {
		return true
	}
	return false
}

func isFollower(phase int) bool {
	if phase==follower_phase {
		return true
	}
	return false
}



func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}


func kill(id int) {


	err:=cmd[id].Process.Kill()
	if err!=nil {
		log.Println("Error in killing")
		panic("Error in killing")
	}
	cmd[id].Process.Wait()



}


var cmd=make([]*exec.Cmd,NO_OF_SERVERS)

func run(id int,port int,wg *sync.WaitGroup) {




	//fmt.Println("")



	cmd[id]=exec.Command("./main/main",strconv.Itoa(id),strconv.Itoa(port),"/home/rahul/IdeaProjects/cloud/src")
	cmd[id].Stderr=os.Stderr

	//cmd[id].Stdout = os.Stdout



	//fmt.Println("Rahul1")

	cmd[id].Start()
	//fmt.Println("Rahul2")


	cmd[id].Wait()




	time.Sleep(500*time.Millisecond)

	wg.Done()

}


func Check_Sequentiality_of_State_Db_Indices(close_sequence []int,t *testing.T) {
	//fmt.Println(close_sequence," ",len(close_sequence))
	state_db:=make([]*leveldb.DB,len(close_sequence))
	er:=make([]error,len(close_sequence))
	for i:=0;i<len(close_sequence);i++ {
		state_db_nm:="../raft/log/state"+strconv.Itoa(close_sequence[i])

		state_db[i],er[i] = leveldb.OpenFile(state_db_nm, nil)

		if er[i]!=nil {
			t.Log(" ",er)
			panic("db error")
		}
		//fmt.Println("Came here1")
	}
	//fmt.Println("Came here2")
	for i:=0;i<len(close_sequence);i++ {
		cnt:=-1
		max:=-1
		iter := state_db[i].NewIterator(nil, nil)
		for iter.Next() {
			k := iter.Key()
			//v := iter.Value()
			str:=string(k)
			keyIndex,_:=strconv.Atoi(str)
			if max<keyIndex {
				max=keyIndex
			}
			cnt++
		}
		iter.Release()
		err := iter.Error()
		if err!=nil {
			t.Log("",err)
			t.Fail()
		}
		//fmt.Println("Last log indices:",max,cnt)
		if max!=cnt {
			t.Log("Error in checking sequentiality")
			t.Fail()
		}
	}
	for i:=0;i<len(close_sequence);i++ {
		state_db[i].Close()
	}
}

func isByteArraysEqual(v1 []byte,v2 []byte) bool{
	i:=0
	var v11 byte
	var v22 byte
	for {
		if i<len(v1) {
			v11=v1[i]
		} else {
			break
		}
		if i<len(v2) {
			v22=v2[i]
		} else {
			break
		}
		if v11!=v22 {
			return false
		}
		i++

	}

	return true
}
func Check_State_Db_Contents_of_Servers(close_sequence []int,t *testing.T) {
	state_db:=make([]*leveldb.DB,len(close_sequence))
	er:=make([]error,len(close_sequence))
	for i:=0;i<len(close_sequence);i++ {
		state_db_nm:="../raft/log/state"+strconv.Itoa(close_sequence[i])

		state_db[i],er[i] = leveldb.OpenFile(state_db_nm, nil)

		if er[i]!=nil {
			t.Log(" ",er)
			panic("db error1")
		}

	}
	flg:=true
	isEnd:=make([]bool,len(close_sequence))
	n:=0

	for ;flg!=false; {

		var k []byte
		var v []byte
		isSet:=false
		for i:=0;i<len(close_sequence);i++ {

			if isEnd[i]!=true {
				tmpk := []byte(strconv.Itoa(n))
				tmpv,err:=state_db[i].Get(tmpk,nil)

				if err!=nil {
					isEnd[i]=true
				} else {
					if isSet==false {
						isSet=true
						k=copyByteArray(tmpk)
						v=copyByteArray(tmpv)
					} else {
						if !(isByteArraysEqual(tmpk,k) && isByteArraysEqual(tmpv,v)) {

							t.Log("Error in checking state_db contents tmpk:",tmpk," tmpv:",tmpv," k:",k," v:",v," i:",i)
							t.Fail()
						}
					}
				}

			}

		}



		cnt:=0
		for i:=0;i<len(close_sequence);i++ {
			if isEnd[i]==true {

				cnt++
			}
		}
		if cnt==len(close_sequence) {
			flg=false
		}

		n++
	}

	for i:=0;i<len(close_sequence);i++ {
		state_db[i].Close()
	}

}


func reverse(abc []int) []int {
	l:=len(abc)
	pqr:=make([]int,l)
	for i:=0;i<l;i++ {
		pqr[i]=abc[i]
	}
	for i:=0;i<len(abc)/2 ;i++ {
		swap:=pqr[i]
		pqr[i]=pqr[l-i-1]
		pqr[l-i-1]=swap
	}
	return pqr
}


//close_Sequence in ascending order of their commitIndices
func Check_KVStore_Contents(close_sequence_asc []int,t *testing.T) {
	close_sequence:=reverse(close_sequence_asc)
	commitIndices:=make([]int,len(close_sequence))
	for i:=0;i<len(close_sequence);i++ {
		commitIndices[i],_=FindCommitIndex(close_sequence[i])

	}
	//fmt.Println(commitIndices)




	kvstore_db:=make([]*leveldb.DB,len(close_sequence))
	er:=make([]error,len(close_sequence))
	for i:=0;i<len(close_sequence);i++ {
		kvstore_db_nm:="../kvstore/kvstore"+strconv.Itoa(close_sequence[i])


		kvstore_db[i],er[i] = leveldb.OpenFile(kvstore_db_nm,nil)

		if er[i]!=nil {
			t.Log(" ",er)
			panic("db error2")
		}

	}
	flg:=true
	isEnd:=make([]bool,len(close_sequence))
	n:=0

	cnt:=make([][]int,len(close_sequence))
	cnt[0]=make([]int,len(close_sequence))
	cnt[1]=make([]int,len(close_sequence))
	cnt[2]=make([]int,len(close_sequence))
	cnt[3]=make([]int,len(close_sequence))
	cnt[4]=make([]int,len(close_sequence))
	for i:=0;i<len(cnt);i++ {
		for j:=0;j<len(cnt[0]);j++ {
			cnt[i][j]=0
		}

	}


	for ;flg!=false; {

		//k:=make([]byte,1000)
		//v:=make([]byte,1000)
		var k []byte
		var v []byte
		lastSetIndex:=-1
		isSet:=false

		for i:=0;i<len(close_sequence);i++ {

			if isEnd[i]!=true {
				tmpk := []byte(strconv.Itoa(n))
				tmpv,err:=kvstore_db[i].Get(tmpk,nil)
				//fmt.Println("K:",i,"tmpk:",tmpk," tmpv:",tmpv)
				if err!=nil {
					isEnd[i]=true
				} else {

					if isSet==false {

						isSet=true
						k=copyByteArray(tmpk)
						v=copyByteArray(tmpv)
						lastSetIndex=i
					} else {

						if (!(isByteArraysEqual(tmpk,k) && isByteArraysEqual(tmpv,v))) {
							cnt[i][lastSetIndex]++
							cnt[lastSetIndex][i]++
							if cnt[i][lastSetIndex]>(commitIndices[i]-commitIndices[lastSetIndex]) {
								t.Log("Error in checking kvstore contents k:",k," v:",v," i:",i," tmpk:",tmpk," tmpv:",tmpv)
								t.Fail()
							}


						}
					}
				}

			}

		}


		for j:=0;j<len(close_sequence);j++ {
			for k:=0;k<len(close_sequence);k++ {
				if isEnd[j]!=isEnd[k] {
					cnt[j][k]++
				}
			}
		}

		cntt:=0
		for k:=0;k<len(close_sequence);k++ {
			if isEnd[k]==true {

				cntt++
			}
		}
		if cntt==len(close_sequence) {
			flg=false
		}

		n++
	}
	for i:=0;i<len(close_sequence);i++ {
		kvstore_db[i].Close()
	}

}

func FindCommitIndex(host_id int) (int,int) {
	max:=-1
	maxterm:=-1


	state_db_nm:="../raft/log/state"+strconv.Itoa(host_id)

	state_db, er := leveldb.OpenFile(state_db_nm, nil)

	if er!=nil {
		log.Println(" ",host_id,er)
		panic("db error3")
	}

	iter := state_db.NewIterator(nil, nil)
	for iter.Next() {
		indexkey := iter.Key()
		v := iter.Value()
		term:=0
		key:=0
		value:=0
		fmt.Sscanf(string(v),"%d,%d,%d",&term,&key,&value)
		k,er:=strconv.Atoi(string(indexkey))

		if er!=nil {
			log.Println("",er)
			panic("Error in findcommitindex")
		}
		if k>max {
			max=k
			maxterm=term
		}

		//fmt.Println(key,value)

	}
	iter.Release()
	errrr := iter.Error()
	if errrr!=nil {
		log.Println("",errrr)
		panic("iter error")
	}
	state_db.Close()
	return max,maxterm
}




func FindLastIndexTmp(host_id int) (int,int) {
	max:=-1
	maxterm:=-1

	tmp_db_nm:="../raft/log/tmp"+strconv.Itoa(host_id)


	tmp_db, errr := leveldb.OpenFile(tmp_db_nm, nil)


	if errr!=nil {
		log.Println(" ",errr)
		panic("db error4")
	}

	iter := tmp_db.NewIterator(nil, nil)
	for iter.Next() {
		indexkey := iter.Key()
		v := iter.Value()
		term:=0
		key:=0
		value:=0
		fmt.Sscanf(string(v),"%d,%d,%d",&term,&key,&value)
		k,er:=strconv.Atoi(string(indexkey))

		if er!=nil {
			log.Println("",er)
			panic("Findcommittmperror")
		}
		if k>max {
			max=k
			maxterm=term
		}

		//fmt.Println(key,value)

	}
	iter.Release()
	errrr := iter.Error()
	if errrr!=nil {
		log.Println(errrr)
		panic("Error in committmpfind")
	}
	tmp_db.Close()
	return max,maxterm
}


func CheckCommitIndices1(close_sequence []int,t *testing.T) {
	maxCommit:=-1
	//indexOfMax:=-1
	for i:=0;i<len(close_sequence);i++ {
		m,_:=FindCommitIndex(close_sequence[i])
		if m>maxCommit {
			maxCommit=m
			//indexOfMax=i
		}

	}

	greater_than_count:=0
	for i:=0;i<len(close_sequence);i++ {
		m,_:=FindLastIndexTmp(close_sequence[i])
		if maxCommit>=m {
			greater_than_count++
		}
	}
	//fmt.Println(greater_than_count," ",)
	if greater_than_count<(MAX_NO_OF_SERVERS/2) {
		t.Log("CheckCommitIndex1TestFailed")
		t.Fail()
	}


}


func CheckCommitIndices2(close_sequence []int,t *testing.T) {
	maxCommit:=-1
	is_not_max_entry:=make([]bool,len(close_sequence))
	m:=make([]int,len(close_sequence))

	//indexOfMax:=-1
	for i:=0;i<len(close_sequence);i++ {
		is_not_max_entry[i]=false
		m[i],_=FindCommitIndex(close_sequence[i])
		if m[i]>maxCommit {
			maxCommit=m[i]
			//indexOfMax=i
		} else {
			is_not_max_entry[i]=true
		}

	}

	for i:=0;i<len(close_sequence);i++ {
		if is_not_max_entry[i]==true {
			cnt:=0
			for j:=0;j<len(close_sequence);j++ {
				if m[i]<=m[j] {
					cnt++
				}

			}
			if cnt<(len(close_sequence)/2) {
				t.Log("CheckCommitIndex2TestFailed")
				t.Fail()
			}
		}
	}



}





/*
func TestDelayedStartOfTwo(t *testing.T) {
	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}


	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<3;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)
	go run(3,rpc_port[3],wg)
	go run(4,rpc_port[4],wg)

	time.Sleep(10*time.Second)

	no_of_phase_requests:=10



	last_leader:=-1


	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}


		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}




	kill(0)
	close_sequence[0]=0


	kill(1)
	close_sequence[1]=1


	kill(2)
	close_sequence[2]=2


	kill(3)
	close_sequence[3]=3


	kill(4)
	close_sequence[4]=4

	//fmt.Println("",close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)
	CheckCommitIndices2(close_sequence[0:5],t)


}




func TestDelayedStartOfOne(t *testing.T) {
	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}


	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<4;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}





	no_of_phase_requests:=10



	last_leader:=-1


	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}


		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}

	time.Sleep(20*time.Second)

	go run(4,rpc_port[4],wg)

	time.Sleep(10*time.Second)


	kill(0)
	close_sequence[0]=0


	kill(1)
	close_sequence[1]=1


	kill(2)
	close_sequence[2]=2


	kill(3)
	close_sequence[3]=3


	kill(4)
	close_sequence[4]=4

	//fmt.Println("",close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)
	CheckCommitIndices2(close_sequence[0:5],t)




}



func TestLeadersClosedOneAfterOtherAndRevoked(t *testing.T) {
	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(8)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last1_leader:=-1


	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last1_leader=i
				//fmt.Println("WithoutKilled:",last1_leader," ",j)
			}



		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last1_leader==-1 {
		t.Log(strconv.Itoa(last1_leader))
		t.Log("No closed Error")
		t.Fail()
	}

	time.Sleep(10*time.Second)
	//kill(rpc_port[last_leader])
	kill(last1_leader)
	cntt++
	close_sequence[cntt]=last1_leader
	time.Sleep(10*time.Second)

	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	last2_leader:=-1
	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last2_leader=i
					//fmt.Println("1killed:",last2_leader," ",j)
				}


			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}



	kill(last2_leader)
	cntt++
	close_sequence[cntt]=last2_leader

	time.Sleep(10*time.Second)



	last3_leader:=-1
	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last3_leader=i
					//fmt.Println("1killed:",last3_leader," ",j)
				}


			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}



	kill(last3_leader)
	cntt++
	close_sequence[cntt]=last3_leader

	time.Sleep(10*time.Second)





	go run(last1_leader,rpc_port[last1_leader],wg)

	time.Sleep(10*time.Second)

	go run(last2_leader,rpc_port[last2_leader],wg)

	time.Sleep(10*time.Second)

	go run(last3_leader,rpc_port[last3_leader],wg)

	time.Sleep(10*time.Second)

	kill(0)
	close_sequence[0]=0


	kill(1)
	close_sequence[1]=1


	kill(2)
	close_sequence[2]=2


	kill(3)
	close_sequence[3]=3


	kill(4)
	close_sequence[4]=4

	//fmt.Println("",close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)
	CheckCommitIndices2(close_sequence[0:5],t)



}
*/


func TestTerminationOfFollowersAndRevokedAgain(t *testing.T) {
	mytestTerminationOfFollowers(t)
	time.Sleep(10*time.Second)
	mytestTerminationOfFollowers(t)
}

func mytestTerminationOfFollowers(t *testing.T) {
	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last_leader:=-1
	last_follower:=-1

	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}
			if isFollower(phase) {
				last_follower=i
			}


		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}


	if last_follower!=-1 {
		kill(last_follower)
		cntt++
		close_sequence[cntt]=last_follower
	}

	time.Sleep(10*time.Second)
	last_leader=-1
	last_follower=-1
	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("1killed:",last_leader," ",j)
				}
				if isFollower(phase) {
					last_follower=i
				}
			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(test_success_cnt,":",no_of_phase_requests)
		t.Log("1 closed Error")
		t.Fail()
	}

	if last_follower!=-1 {
		kill(last_follower)
		cntt++
		close_sequence[cntt]=last_follower
	}


	time.Sleep(10*time.Second)
	last_leader=-1
	last_follower=-1

	//fmt.Printf("\nAfter deleting %d",last_leader)//2


	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("2killed:",last_leader," ",j)
				}

				if isFollower(phase) {
					last_follower=i
				}
			}

		}
		if leader_cnt==1{
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}


	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("2 closed error")
		t.Fail()
	}


	if last_follower!=-1 {
		kill(last_follower)
		cntt++
		close_sequence[cntt]=last_follower
	}

	time.Sleep(10*time.Second)

	last_leader=-1
	last_follower=-1



	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {

			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("3killed:",last_leader," ",j)
				}

				if isFollower(phase) {
					last_follower=i
				}
			}


		}
		if leader_cnt==1 {
			test_success_cnt++
		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}





	for i:=0;i<5;i++ {

		//fmt.Println(i," ",cntt," ",len(close_sequence[0:cntt+1]))
		if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
			kill(i)
			cntt++
			close_sequence[cntt]=i

		}
	}


	//fmt.Println(close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)

	CheckCommitIndices2(close_sequence[0:5],t)








}






func TestMajorityMinorityTerminatingFollowersOneByOne(t *testing.T) {

	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last_leader:=-1
	last_follower:=-1

	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}
			if isFollower(phase) {
				last_follower=i
			}


		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}


	if last_follower!=-1 {
		kill(last_follower)
		cntt++
		close_sequence[cntt]=last_follower
	}

	time.Sleep(10*time.Second)
	last_leader=-1
	last_follower=-1
	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("1killed:",last_leader," ",j)
				}
				if isFollower(phase) {
					last_follower=i
				}
			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}

	if last_follower!=-1 {
		kill(last_follower)
		cntt++
		close_sequence[cntt]=last_follower
	}


	time.Sleep(10*time.Second)
	last_leader=-1
	last_follower=-1

	//fmt.Printf("\nAfter deleting %d",last_leader)//2


	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("2killed:",last_leader," ",j)
				}

				if isFollower(phase) {
					last_follower=i
				}
			}

		}
		if leader_cnt==1{
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}


	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("2 closed error")
		t.Fail()
	}


	if last_follower!=-1 {
		kill(last_follower)
		cntt++
		close_sequence[cntt]=last_follower
	}

	time.Sleep(10*time.Second)

	last_leader=-1
	last_follower=-1



	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {

			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("3killed:",last_leader," ",j)
				}

				if isFollower(phase) {
					last_follower=i
				}
			}


		}
		if leader_cnt==1 {
			test_success_cnt++
		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}





	for i:=0;i<5;i++ {

		//fmt.Println(i," ",cntt," ",len(close_sequence[0:cntt+1]))
		if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
			kill(i)
			cntt++
			close_sequence[cntt]=i

		}
	}


	//fmt.Println(close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)

	CheckCommitIndices2(close_sequence[0:5],t)








}

/*
func TestMajorityMinorityTerminatingLeaderOneByOne(t *testing.T) {

	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last_leader:=-1


	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}



		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}


	//kill(rpc_port[last_leader])
	kill(last_leader)

	cntt++
	close_sequence[cntt]=last_leader

	time.Sleep(10*time.Second)

	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("1killed:",last_leader," ",j)
				}


			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}


	//kill(rpc_port[last_leader])
	kill(last_leader)

	cntt++
	close_sequence[cntt]=last_leader

	time.Sleep(10*time.Second)

	//fmt.Printf("\nAfter deleting %d",last_leader)//2
	last_leader=-1



	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("2killed:",last_leader," ",j)
				}


			}

		}
		if leader_cnt==1{
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}


	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("2 closed error")
		t.Fail()
	}


	//kill(rpc_port[last_leader])
	kill(last_leader)


	cntt++
	close_sequence[cntt]=last_leader

	time.Sleep(10*time.Second)

	last_leader=-1




	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {

			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("3killed:",last_leader," ",j)
				}
			}


		}
		if leader_cnt==1 {
			test_success_cnt++
		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}
	if(test_success_cnt!=0) {
		t.Log("3 closed error")
		t.Fail()
	}


	for i:=0;i<5;i++ {

		//fmt.Println(i," ",cntt," ",len(close_sequence[0:cntt+1]))
		if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
			kill(i)
			cntt++
			close_sequence[cntt]=i

		}
	}


	//fmt.Println(close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)

	CheckCommitIndices2(close_sequence[0:5],t)








}
*/

/*
func TestTerminaatingRandomlyAndInvoke(t *testing.T) {
	mytestMajorityMinorityTerminatingRandomly(t)
	mytestMajorityMinorityTerminatingRandomly(t)
}

func mytestMajorityMinorityTerminatingRandomly(t *testing.T) {

	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last_leader:=-1


	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}


		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}



	kill(0)
	cntt++
	close_sequence[cntt]=0


	time.Sleep(10*time.Second)
	last_leader=-1

	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("1killed:",last_leader," ",j)
				}

			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}


	kill(1)
	cntt++
	close_sequence[cntt]=1



	time.Sleep(10*time.Second)
	last_leader=-1


	//fmt.Printf("\nAfter deleting %d",last_leader)//2


	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("2killed:",last_leader," ",j)
				}


			}

		}
		if leader_cnt==1{
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}


	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("2 closed error")
		t.Fail()
	}



	kill(2)
	cntt++
	close_sequence[cntt]=2


	time.Sleep(10*time.Second)

	last_leader=-1




	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {

			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("3killed:",last_leader," ",j)
				}


			}


		}
		if leader_cnt==1 {
			test_success_cnt++
		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}



	kill(3)
	cntt++
	close_sequence[cntt]=3


	kill(4)
	cntt++
	close_sequence[cntt]=4




	//fmt.Println(close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)

	CheckCommitIndices2(close_sequence[0:5],t)








}
*/
/*
func TestMajorityMinorityTerminatingRandomly(t *testing.T) {

	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last_leader:=-1


	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}


		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}



	kill(0)
	cntt++
	close_sequence[cntt]=0


	time.Sleep(10*time.Second)
	last_leader=-1

	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("1killed:",last_leader," ",j)
				}

			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}


	kill(1)
	cntt++
	close_sequence[cntt]=1



	time.Sleep(10*time.Second)
	last_leader=-1


	//fmt.Printf("\nAfter deleting %d",last_leader)//2


	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("2killed:",last_leader," ",j)
				}


			}

		}
		if leader_cnt==1{
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}


	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("2 closed error")
		t.Fail()
	}



	kill(2)
	cntt++
	close_sequence[cntt]=2


	time.Sleep(10*time.Second)

	last_leader=-1




	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {

			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("3killed:",last_leader," ",j)
				}


			}


		}
		if leader_cnt==1 {
			test_success_cnt++
		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}



	kill(3)
	cntt++
	close_sequence[cntt]=3


	kill(4)
	cntt++
	close_sequence[cntt]=4




	//fmt.Println(close_sequence[0:5])




	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)

	CheckCommitIndices2(close_sequence[0:5],t)








}
*/
/*
func TestFollowersClosedAndAtTheSameTimeRevoked(t *testing.T) {
	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(9)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last_leader:=-1
	last1_follower:=-1

	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}
			if isFollower(phase) {
				last1_follower=i
			}


		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}


	if last1_follower!=-1 {
		kill(last1_follower)
		cntt++
		close_sequence[cntt]=last1_follower
	}

	time.Sleep(10*time.Second)
	last_leader=-1
	last2_follower:=-1
	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("1killed:",last_leader," ",j)
				}
				if isFollower(phase) {
					last2_follower=i
				}
			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}

	if last2_follower!=-1 {
		kill(last2_follower)
		cntt++
		close_sequence[cntt]=last2_follower
	}


	time.Sleep(10*time.Second)
	last_leader=-1
	last3_follower:=-1

	//fmt.Printf("\nAfter deleting %d",last_leader)//2


	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("2killed:",last_leader," ",j)
				}

				if isFollower(phase) {
					last3_follower=i
				}
			}

		}
		if leader_cnt==1{
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}


	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("2 closed error")
		t.Fail()
	}


	if last3_follower!=-1 {
		kill(last3_follower)
		cntt++
		close_sequence[cntt]=last3_follower
	}

	time.Sleep(10*time.Second)

	last_leader=-1
	last4_follower:=-1



	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {

			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("3killed:",last_leader," ",j)
				}

				if isFollower(phase) {
					last4_follower=i
				}
			}


		}
		if leader_cnt==1 {
			test_success_cnt++
		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}



	if last4_follower!=-1 {
		kill(last4_follower)
		cntt++
		close_sequence[cntt]=last4_follower
	}


	if last1_follower!=-1 {
		go run(last1_follower,rpc_port[last1_follower],wg)
	}
	if last2_follower!=-1 {
		go run(last2_follower,rpc_port[last2_follower],wg)
	}
	if last3_follower!=-1 {
		go run(last3_follower,rpc_port[last3_follower],wg)
	}
	if last4_follower!=-1 {
		go run(last4_follower,rpc_port[last4_follower],wg)
	}

	//fmt.Println("Close seq:",close_sequence)
	time.Sleep(20*time.Second)



	kill(0)
	close_sequence[0]=0

	kill(1)
	close_sequence[1]=1

	kill(2)
	close_sequence[2]=2

	kill(3)
	close_sequence[3]=3

	kill(4)
	close_sequence[4]=4









	Check_State_Db_Contents_of_Servers(close_sequence[0:5],t)
	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)

	CheckCommitIndices2(close_sequence[0:5],t)



}
*/




/*
func TestTerminationOfLeadersAndRevokedAgain(t *testing.T)() {
	mytestMajorityMinorityTerminatingLeaderOneByOne(t)
	t.Log("First part passed")
	time.Sleep(5*time.Second)
	mytestMajorityMinorityTerminatingLeaderOneByOne(t)
}

func mytestMajorityMinorityTerminatingLeaderOneByOne(t *testing.T) {

	close_sequence:=make([]int,5)
	for i:=0;i<5;i++ {
		close_sequence[i]=-1
	}
	cntt:=-1

	wg:=new(sync.WaitGroup)
	wg.Add(5)

	rpc_port:=make([]int,5)



	rpc_port[0]=11110
	rpc_port[1]=11111
	rpc_port[2]=11112
	rpc_port[3]=11113
	rpc_port[4]=11114


	for i:=0;i<5;i++ {
		//fmt.Println("Called for:",i)
		go run(i,rpc_port[i],wg)
	}

	time.Sleep(20*time.Second)



	no_of_phase_requests:=10



	last_leader:=-1


	leader_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			phase:=getPhase(rpc_port[i])
			if isLeader(phase) {
				leader_cnt++
				last_leader=i
				//fmt.Println("WithoutKilled:",last_leader," ",j)
			}



		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}




	if last_leader==-1 {
		t.Log(strconv.Itoa(last_leader))
		t.Log("No closed Error")
		t.Fail()
	}


	//kill(rpc_port[last_leader])
	kill(last_leader)

	cntt++
	close_sequence[cntt]=last_leader

	time.Sleep(10*time.Second)

	//fmt.Printf("\nAfter deleting %d",last_leader)//1


	leader_cnt=0
	test_success_cnt:=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("1killed:",last_leader," ",j)
				}


			}

		}

		if leader_cnt==1 {
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}

	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("1 closed Error")
		t.Fail()
	}


	//kill(rpc_port[last_leader])
	kill(last_leader)

	cntt++
	close_sequence[cntt]=last_leader

	time.Sleep(10*time.Second)

	//fmt.Printf("\nAfter deleting %d",last_leader)//2
	last_leader=-1



	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {
			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("2killed:",last_leader," ",j)
				}


			}

		}
		if leader_cnt==1{
			test_success_cnt++
		}

		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}


	if test_success_cnt!=no_of_phase_requests {
		t.Log(strconv.Itoa(test_success_cnt)+":"+strconv.Itoa(no_of_phase_requests))
		t.Log("2 closed error")
		t.Fail()
	}


	//kill(rpc_port[last_leader])
	kill(last_leader)


	cntt++
	close_sequence[cntt]=last_leader

	time.Sleep(10*time.Second)

	last_leader=-1




	leader_cnt=0
	test_success_cnt=0
	for j:=0;j<no_of_phase_requests;j++ {
		leader_cnt=0
		for i:=0;i<5;i++ {

			if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
				phase:=getPhase(rpc_port[i])
				if isLeader(phase) {
					leader_cnt++
					last_leader=i
					//fmt.Println("3killed:",last_leader," ",j)
				}
			}


		}
		if leader_cnt==1 {
			test_success_cnt++
		}
		//fmt.Printf("\nNumber of leaders:%d",cnt)
		time.Sleep(500*time.Millisecond)
	}
	if(test_success_cnt!=0) {
		t.Log("3 closed error")
		t.Fail()
	}


	for i:=0;i<5;i++ {

		//fmt.Println(i," ",cntt," ",len(close_sequence[0:cntt+1]))
		if !isInAnotherArray(i,close_sequence[0:cntt+1]) {
			kill(i)
			cntt++
			close_sequence[cntt]=i

		}
	}


	Check_Sequentiality_of_State_Db_Indices(close_sequence[0:5],t)
	Check_KVStore_Contents(close_sequence[0:5],t)
	CheckCommitIndices1(close_sequence[0:5],t)

	CheckCommitIndices2(close_sequence[0:5],t)








}


*/

func isInAnotherArray(sval int,valarr []int) bool {
	for i:=0;i<len(valarr);i++ {
		if sval==valarr[i] {
			return true
		}
	}
	return false
}
func copyByteArray(src []byte) []byte {
	dst:=make([]byte,len(src))
	for i:=0;i<len(src);i++ {
		dst[i]=src[i]
	}
	return dst
}
