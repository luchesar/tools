package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jmcvetta/napping"
	"github.com/libgit2/git2go"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"os"
	"path"
	"time"
	"log"
        "runtime"
)

const (
	MasterRef = "refs/heads/master"
)

var mongoConnection = "mongodb://54.171.48.210/coinvision"
var mongoDb = "coinvision"
var mongoCollection = "bitfinex_l2"

func main() {
        var gitRepoPath = os.Args[1]
        migrateMongo(gitRepoPath)
        fetchForever(gitRepoPath)
}

func fetchForever(gitRepoPath string) {
	gitRepo, err := git.OpenRepository(gitRepoPath); Try(err)
	defer gitRepo.Free()

	ticker := time.NewTicker(time.Second)
	books := make(chan map[string]interface{})

	go func() {
		for {
			updateFile(gitRepoPath, gitRepo, <-books)
		}
	}()

	for {
		<-ticker.C
		go fetchBitFinextOrderBook(books)
	}
}

func fetchBitFinextOrderBook(books chan map[string]interface{}) {
	defer Catch("Cannot fetch bitfinex order book")
	s := napping.Session{}
	params := napping.Params{"group": "0"}
	var res map[string]interface{}
	resp, err := s.Get("https://api.bitfinex.com/v1/book/btcusd", &params, &res, nil); Try(err)
	if resp.Status() == 200 {
		now := time.Now().UnixNano()
		res["timestamp"] = now
		fmt.Println("successfully downloaded order book at Unix time ", now)
		books <- res
	} else {
		fmt.Println("Failed to get the order book from Bitfinex")
	}
}

func migrateMongo(gitRepoPath string) {
	sess, err := mgo.Dial(mongoConnection); Try(err)
	defer sess.Close()

	gitRepo, err := git.OpenRepository(gitRepoPath); Try(err)
	defer gitRepo.Free()

	sess.SetSafe(&mgo.Safe{})
	collection := sess.DB(mongoDb).C(mongoCollection)

	var result map[string]interface{}
	iter := collection.Find(nil).Iter()
	defer Try(iter.Close())
	for iter.Next(&result) {
                fmt.Printf("Order book fetched from Mongodb: %v\n", result["timestamp"])
		updateFile(gitRepoPath, gitRepo, result)
	}
}

func updateFile(gitRepoPath string, gitRepo *git.Repository, book map[string]interface{}) {
	defer Catch("Cannot handle a record")
	b, err := json.MarshalIndent(book, "", " "); Try(err)
	var filePath = path.Clean(path.Dir(gitRepoPath) + "/" + mongoCollection)
	Try(ioutil.WriteFile(filePath, b, 0644))
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

	master, err := repo.LookupReference(MasterRef); Try(err)
	defer master.Free()

	objId := master.Target()
	c, err := repo.LookupCommit(objId); Try(err)

	parentCommit = append(parentCommit, c)
	_, err = repo.CreateCommit(MasterRef, signature, signature, message, tree, parentCommit...); Try(err)
}

func gitAdd(repo *git.Repository, paths ...string) *git.Tree {
	indx, err := repo.Index(); Try(err)
	defer indx.Free()

	// Go through all the filepaths and add them to the indx
	for _, file := range paths {
		Try(indx.AddByPath(file))
	}

	Try(indx.Write())
	treeID, err := indx.WriteTree(); Try(err)
	tree, err := repo.LookupTree(treeID); Try(err)
	return tree
}
func Try(err error) {
	if err != nil {
		panic(err)
	}
}

func Catch(message string) {
	if r := recover(); r != nil {
		_, fn, line, _ := runtime.Caller(1)
		log.Printf("[error] %s:%d %v", fn, line, r)
	}
}
