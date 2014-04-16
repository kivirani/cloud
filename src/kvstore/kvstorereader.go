package main
import (

	"fmt"
	"os"
	"strconv"
	"github.com/syndtr/goleveldb/leveldb"



)
var flg=0
const (
	MAX=200
)
func new() * DbReader {
	host_id,_:=strconv.Atoi(os.Args[1])


	kvstore_db_nm:="../kvstore/kvstore"+strconv.Itoa(host_id)
	new_kvstore_db, ere := leveldb.OpenFile(kvstore_db_nm, nil)
	if ere!=nil {
		fmt.Println(" ",ere)
		panic("db error")
	}

	r:=DbReader{kvstore_db:new_kvstore_db}
	return &r

}

type DbReader struct {
	kvstore_db *leveldb.DB
}

func (r *DbReader) ShowKVStoreDbContents() {
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

func isByteArraysEqual(v1 []byte,v2 []byte) bool{
	i:=0
	if len(v1)!=len(v2) {
		return false
	}
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
func main() {





	fmt.Println("KVstorecontents")
	r:=new()

	r.ShowKVStoreDbContents()










}

