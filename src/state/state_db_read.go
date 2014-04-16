package main
import (

	"fmt"
	"os"
	"strconv"
	"kivirani/cloud/github.com/syndtr/goleveldb/leveldb"


)
var flg=0
const (
	MAX=200
)
func new() * DbReader {
	host_id,_:=strconv.Atoi(os.Args[1])
	tmp_db_nm:="../raft/log/tmp"+strconv.Itoa(host_id)
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
	r:=DbReader{tmp_db:new_tmp_db,state_db:new_state_db}
	return &r

}

type DbReader struct {
	tmp_db *leveldb.DB
	state_db *leveldb.DB

}

func (r *DbReader) getFromTmpDB(key []byte) []byte {
	value, err := r.tmp_db.Get(key, nil)
	if err!=nil {
		return value
	}
	return value
}
func (r *DbReader) FindCommitIndex() (int,int) {
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

func (r *DbReader) FindLastIndexTmp() (int,int) {
	max:=-1
	maxterm:=-1
	iter := r.tmp_db.NewIterator(nil, nil)
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

func (r *DbReader) ShowTmpDbContents() {
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

func (r *DbReader) ShowStateDbContents() {
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


func main() {





	fmt.Println("StateDBContents")
	r:=new()
	key:=[]byte(strconv.Itoa(1))
	s:=""
	a:=-1
	b:=-1
	c:=-1
	s=string(r.getFromTmpDB(key))
	fmt.Sscanf(s,"%d,%d,%d",&a,&b,&c)
	fmt.Println(a," ",b," ",c," ",s)
	r.ShowStateDbContents()
	//fmt.Println("TmpDBContents")
	//	r.ShowTmpDbContents()
	//r.RecomputeParameters()

	//	k,t:=r.FindCommitIndexTmp()
	k,t:=r.FindCommitIndex()
	fmt.Println("   ",k,t)

}

