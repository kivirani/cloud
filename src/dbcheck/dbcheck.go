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
	"kivirani/cloud/github.com/syndtr/goleveldb/leveldb"
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




	cmd:=exec.Comm
