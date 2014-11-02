package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jmcvetta/napping"
	"github.com/libgit2/git2go"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"time"
)

const (
	MasterRef = "refs/heads/master"
)

var mongoConnection = "mongodb://54.171.48.210/coinvision"
var mongoDb = "coinvision"
var mongoCollection = "bitfinex_l2"

var gitRepoPath = os.Args[1]

func main() {
	gitRepo, err := git.OpenRepository(gitRepoPath)
	try(err)
	defer gitRepo.Free()

	ticker := time.NewTicker(time.Second)
	books := make(chan map[string]interface{})

	go func() {
		for {
			updateFile(gitRepo, <-books)
		}
	}()

	for {
		<-ticker.C
		go fetchBitFinextOrderBook(books)
	}
}

func fetchBitFinextOrderBook(books chan map[string]interface{}) {
	s := napping.Session{}
	params := napping.Params{"group": "0"}
	var res map[string]interface{}
	resp, err := s.Get("https://api.bitfinex.com/v1/book/btcusd", &params, &res, nil)
	try(err)
	if resp.Status() == 200 {
		now := time.Now().UnixNano()
		res["timestamp"] = now
		fmt.Println("successfully downloaded order book at Unix time ", now)
		books <- res
	} else {
		fmt.Println("Failed to get the order book from Bitfinex")
	}
}

func migrateMongo() {
	sess, err := mgo.Dial(mongoConnection)
	try(err)
	defer sess.Close()

	gitRepo, err := git.OpenRepository(gitRepoPath)
	try(err)
	defer gitRepo.Free()

	sess.SetSafe(&mgo.Safe{})
	collection := sess.DB(mongoDb).C(mongoCollection)

	var result map[string]interface{}
	iter := collection.Find(nil).Iter()
	defer try(iter.Close())
	for i := 0; i < 5; i++ {
		iter.Next(&result)
		updateFile(gitRepo, result)
	}
}

func updateFile(gitRepo *git.Repository, book map[string]interface{}) {
	defer catch("Cannot handle a record")
	b, err := json.MarshalIndent(book, "", " ")
	try(err)
	var filePath = path.Clean(path.Dir(gitRepoPath) + "/" + mongoCollection)
	try(ioutil.WriteFile(filePath, b, 0644))
	fmt.Printf("file %s written with size %d and timestamp %v\n", filePath, len(b), book["timestamp"])

	gitAddAndCommit(gitRepo, []string{mongoCollection}, "book id "+fmt.Sprintf("%v", book["_id"]))
}

func gitAddAndCommit(repo *git.Repository, paths []string, message string) {
	if message == "" {
		panic(errors.New("commit message empty"))
	}

	tree := gitAdd(repo, paths...)
	defer tree.Free()

	gitCommit(repo, message, tree)
}

func gitCommit(repo *git.Repository, message string, tree *git.Tree) {
	signature := &git.Signature{
		Name:  "lucho",
		Email: "luchesar.cekov@gmail.com",
		When:  time.Now(),
	}
	parentCommit := make([]*git.Commit, 0)

	master, err := repo.LookupReference(MasterRef)
	try(err)
	defer master.Free()

	objId := master.Target()
	c, err := repo.LookupCommit(objId)
	try(err)

	parentCommit = append(parentCommit, c)
	_, err = repo.CreateCommit(MasterRef, signature, signature, message, tree, parentCommit...)
	try(err)
}

func gitAdd(repo *git.Repository, paths ...string) *git.Tree {
	indx, err := repo.Index()
	try(err)
	defer indx.Free()

	// Go through all the filepaths and add them to the indx
	for _, file := range paths {
		try(indx.AddByPath(file))
	}

	try(indx.Write())
	treeID, err := indx.WriteTree()
	try(err)
	tree, err := repo.LookupTree(treeID)
	try(err)
	return tree
}

func try(err error) {
	if err != nil {
		panic(err)
	}
}

func catch(message string) {
	if r := recover(); r != nil {
		_, fn, line, _ := runtime.Caller(1)
		log.Printf("[error] %s:%d %v", fn, line, r)
	}
}
