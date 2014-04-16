package Raft


import (
	//"encoding/xml"
	//"fmt"
	//"os"
	"strconv"
	//"strings"
)

type RequestVote struct {
	XMLName int `xml:"requestvote"`
	Term int `xml:"term"`
	Candidateid int `xml:"candidateid"`
	Lastlogindex int `xml:"lastlogindex"`
	Lastlogterm int `xml:"lastlogterm"`
}

func generateXmlRequestVote (term int,candidateId int,lastLogIndex int,lastLogTerm int) string{
	return `<requestvote><term>`+strconv.Itoa(term)+`</term><candidateid>`+strconv.Itoa(candidateId)+`</candidateid><lastlogindex>`+strconv.Itoa(lastLogIndex)+`</lastlogindex><lastlogterm>`+strconv.Itoa(lastLogTerm)+`</lastlogterm></requestvote>`
}

type RequestVoteResponse struct {
	XMLName int `xml:"requestvoteresponse"`
	Term int `xml:"term"`
	Votegranted string `xml:"votegranted"`
}
func generateXmlRequestVoteResponse(term int,voteGranted string) string {
	return `<requestvoteresponse><term>`+strconv.Itoa(term)+`</term><votegranted>`+voteGranted+`</votegranted></requestvoteresponse>`
}

type Log_Entry struct {
	Logindex int `xml:"logindex"`
	Term int `xml:"term"`
	Key string `xml:"key"`
	Value string `xml:"value"`
}

type AppendEntries struct {
	XMLName int `xml:"appendentries"`
	Term int `xml:"term"`
	Leaderid int `xml:"leaderid"`
	Prevlogindex int `xml:"prevlogindex"`
	Prevlogterm int `xml:"prevlogterm"`
	Logentry []Log_Entry `xml:"logentry"`
	Leadercommit int `xml:"leadercommit"`
}

func generateXmlAppendEntries (term int, leaderId int,prevLogIndex int, prevLogTerm int,logentry []Log_Entry,leaderCommit int) string {
	//return `<appendentries><term>`+strconv.Itoa(term)+`</term><leaderid>`+strconv.Itoa(leaderId)+`</leaderid><prevlogindex>`+strconv.Itoa(prevLogIndex)+`</prevlogindex><prevlogterm>`+strconv.Itoa(prevLogTerm)+`</prevlogterm><logentry><logindex>0</logindex><term>0</term><key>abc</key><value>asdadad</value></logentry><leadercommit>`+strconv.Itoa(leaderCommit)+`</leadercommit></appendentries>`
	str:=`<appendentries><term>`+strconv.Itoa(term)+`</term><leaderid>`+strconv.Itoa(leaderId)+`</leaderid><prevlogindex>`+strconv.Itoa(prevLogIndex)+`</prevlogindex><prevlogterm>`+strconv.Itoa(prevLogTerm)+`</prevlogterm>`
	for i:=0;i<len(logentry);i++ {
		str=str+`<logentry><logindex>`+strconv.Itoa(logentry[i].Logindex)+`</logindex><term>`+strconv.Itoa(logentry[i].Term)+`</term><key>`+logentry[i].Key+`</key><value>`+logentry[i].Value+`</value></logentry>`
	}
	str=str+`<leadercommit>`+strconv.Itoa(leaderCommit)+`</leadercommit></appendentries>`
	return str
}



type AppendEntriesResponse struct {
	XMLName int `xml:"appendentriesresponse"`
	Term int `xml:"term"`
	Success string `xml:"success"`
}
func generateXmlAppendEntriesResponse (term int,success string) string {

	return `<appendentriesresponse><term>`+strconv.Itoa(term)+`</term><success>`+success+`</success></appendentriesresponse>`
}
