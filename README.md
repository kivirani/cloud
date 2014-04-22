cloud
=====

Contains base layers for cloud- raft and cluster with kvstore using levelDB

`cloud` is a golang code - a raft protocol code which simulates the behaviour of [raft](https://speakerdeck.com/benbjohnson/raft-the-understandable-distributed-consensus-protocol) protocol. It consists of number of components. My current implementation contains 5 servers which are connected to each other. The leader election part of raft ensures there is only one leader. And leader will perform all write operations requested by clients. Clients must request server for performing the operation. In my current implementation, server accepts only set operations and I haven't care about the get operations. The get operations can be served by any server inn the network. The cluster layer at the bottom of the hierarchy is used for basic server communication.   

#Usage

(note: In the examples below, $ is the shell prompt, and the output of the snippet follows "----------------"
#### Testing the package
```
Raft$ go test
---------------------------------
PASS
ok    Raft   141.193s
```
This program performs number of tests that ensures that the code is runnung fine. The tests contains the various types of failure conditions and code checks for the availability (atleast one leader), durability(whether changes reflected in state), consistency (eventual consistency). The test doesn't contain the partitioning tests (when network gets partitoned). But I have taken care of the conditions like this in my code. So, don't worry. 
The program may take several minutes to run. So, please don't stop it in between. Because of the system problem in executing the files number of times in a sequence, the current test program tests for only one codition. The test program contains many tests which can be enabled by decommenting the test code in raft_test.go file. The running of one test at a time is recommended. But the program is running fine most of the time when you are running bulk tests. The error in test code doesn't ensure error in original code. You can verify this by running the scenarios written in the testcases. This project uses leveldb as a database.


### Running the packages

```
Raft$ go run main.go 0 11110 /home/rahul/IdeaProjects/cloud/src

```
Or
```
Raft$ ./main/main 0 11110 /home/rahul/IdeaProjects/cloud/src
```

The first parameter is `id`, second one is `port` and third parameter is the `parent directory` (path upto the src directory). This program runs the program for the above parameters. The configuration for raft are written in `configuration_.xml` files and configuration for cluster package are written in `serverlist_.xml` 
Currently program is written for `5` servers which can be converted into more by modifying the configuration files for above two packages. The programs currently running for following `serverid` and `port` combination.
```
serverid    port
-----------------
0           11110
1           11111
2           11112
3           11113
4           11114
5           11115
```

# Install
Set first GOPATH variable

```
$export GOPATH=/path

```
'/path' is the gopath.

Install required repositories first 

```
$go get github.com/pebbe/zmq4
$go get github.com/syndtr/goleveldb/leveldb

```

And then upload this repository using

```
go get github.com/kivirani/cloud

```
### The `cloud/cluster` package

This package contains cluster layer which is used in making & sending & receiving the messages.

### The `cloud/Raft` package

This package contains raft `leader election` layer which will ensure that only one leader at a time. This part simulates the `raft` protocol. The second functionality of this layer is `log replication`. Leader ensures that all the logs get replicated all across the servers and commits the entries only when majority of the servers got these log entries.

### The `cloud/dbcheck`
This package contains code to check the dbcontents after executing any scenario. No need to explicitly run this if you execute test program as test program already contains this functionality.

```
dbcheck$go run dbcheck.go 0

```
To check database contents of log. Here '0' is the PID of the server. This checks consistency,durability.
### The `cloud/kvstore`
Contains the code to see the kvstore contents and this is the place of storing kvstore.

```
kvstore$go run kvstorereader.go 0

```
Here '0' is the PID of the server.

### The `cloud/state`
Contains the code to see the statelog contents.

```
state$go run state_db_read.go 0

```
Here '0' is the PID of the server.

### The `cloud/tmp`
Contains the code to see the tmplog contents.

```
tmp$go run tmp_db_read.go 0

```
Here the '0' is the PID of the server.


### The client
The current client program is embedded in `raft.go` (function name-`startReceivingClientRequests()`) . After going to leader phase it starts running and stops when leader  goes to another phase(follower). 
In addition `raft/log` is the location of storing the leveldb log databases.
# How it works
For each server a `main` program initializes the server. This program by using `raft` package creates the raft instance and starts its execution. The raft instance created will eventually create the `cluster` instance which will bind network socket with each raft instance.

# License

`cloud` is available free. So, anyone can use this repository or modify it.
