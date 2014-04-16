package main

import (
	"strconv"
	"sync"
	"time"
	"os"
	"os/exec"
	"strings"
	"bytes"
	"path"
	"fmt"
	"log"
	"github.com/syndtr/goleveldb/leveldb"
)

var MAX_NO_OF_SERVERS=5

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}
func run(id int,port int,wg *sync.WaitGroup) {


	go_exec_path, lookErr := exec.LookPath("go")
	if lookErr != nil {
		panic(lookErr)
	}
	//fmt.Println(""+go_exec_path)


	env := os.Environ()


	curr_path,_:=os.Getwd()
	//fmt.Printf(""+path)
	parent,_:=path.Split(curr_path)
	parent=TrimSuffix(parent,"/")
	//fmt.Println("Parent"+parent)




	cmd:=exec.Command("go","run","main.go",strconv.Itoa(id),strconv.Itoa(port),parent)
	cmd.Env=env
	var out bytes.Buffer
	cmd.Stdout = &out

	var errbuff bytes.Buffer
	cmd.Stderr = &errbuff



	//cmd.Dir="/home/rahul/IdeaProjects/cloud/src"
	//cmd.Path="/usr/bin/go"

	cmd.Dir=parent
	cmd.Path=go_exec_path
	cmd.Start()


	cmd.Wait()




	time.Sleep(500*time.Millisecond)

	wg.Done()
}


func Check_Sequentiality_of_State_Db_Indices(close_sequence []int) {
	state_db:=make([]*leveldb.DB,len(close_sequence))
	er:=make([]error,len(close_sequence))
	for i:=0;i<len(close_sequence);i++ {
		state_db_nm:="../raft/log/state"+strconv.Itoa(close_sequence[i])

		state_db[i],er[i] = leveldb.OpenFile(state_db_nm, nil)

		if er[i]!=nil {
			fmt.Println(" ",er)
			//panic("db error")
		}


	}
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
			fmt.Println(err)
		}
		fmt.Println("Last log indices:",max,cnt)
		if max!=cnt {
			log.Println("Error in checking sequentiality")
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
func Check_State_Db_Contents_of_Servers(close_sequence []int) {
	state_db:=make([]*leveldb.DB,len(close_sequence))
	er:=make([]error,len(close_sequence))
	for i:=0;i<len(close_sequence);i++ {
		state_db_nm:="../raft/log/state"+strconv.Itoa(close_sequence[i])


		state_db[i],er[i] = leveldb.OpenFile(state_db_nm, nil)

		if er[i]!=nil {
			fmt.Println(" ",er)
			//panic("db error1")
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

							log.Println("Error in checking state_db contents tmpk:",tmpk," tmpv:",tmpv," k:",k," v:",v," i:",i)
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

//close_Sequence in descending order of their commitIndices
func Check_KVStore_Contents(close_sequence []int) {

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
			fmt.Println(" ",er)
			//panic("db error2")
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
								log.Println("Error in checking kvstore contents k:",k," v:",v," i:",i," tmpk:",tmpk," tmpv:",tmpv)
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
	/*for i:=0;i<len(cnt);i++ {
		for j:=0;j<len(cnt[0]);j++ {
			fmt.Print(cnt[i][j])
		}
		fmt.Println("")

	}*/

}

func FindCommitIndex(host_id int) (int,int) {
	max:=-1
	maxterm:=-1


	state_db_nm:="../raft/log/state"+strconv.Itoa(host_id)

	state_db, er := leveldb.OpenFile(state_db_nm, nil)

	if er!=nil {
		fmt.Println(" ",host_id,er)
		//panic("db error3")
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
			fmt.Println(er)
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
		fmt.Println(errrr)
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
		fmt.Println(" ",errr)
		//panic("db error4")
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
			fmt.Println(er)
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
		fmt.Println(errrr)
	}
	tmp_db.Close()
	return max,maxterm
}


func CheckCommitIndices1(close_sequence []int) {
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
		log.Println("CheckCommitIndex1TestFailed")
	}


}


func CheckCommitIndices2(close_sequence []int) {
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
				log.Println("CheckCommitIndex2TestFailed")
			}
		}
	}



}




func main() {
	v:=make([]int,5)
	v[0]=1
	v[1]=2
	v[2]=3
	v[3]=4
	v[4]=0
	Check_Sequentiality_of_State_Db_Indices(v)
	Check_State_Db_Contents_of_Servers(v)


	CheckCommitIndices1(v)

	CheckCommitIndices2(v)
	Check_KVStore_Contents(v)


}



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
