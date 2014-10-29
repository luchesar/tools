package main

import (
  "fmt"
  "gopkg.in/mgo.v2"
  "io/ioutil"
  "os"
  "encoding/json"
//  "gopkg.in/mgo.v2/bson"
)

var mongoConnection = "mongodb://54.171.48.210/coinvision"
var mongoDb = "coinvision"
var mongoCollection = "bitfinex_l2"
var gitRepoPath = os.Args[1]

func main() {
  sess, err := mgo.Dial(mongoConnection)
  if err != nil {
    fmt.Printf("Can't connect to mongo %v\n", err)
    os.Exit(1)
  }
  defer sess.Close() 

  sess.SetSafe(&mgo.Safe{})
  collection := sess.DB(mongoDb).C(mongoCollection)

  var result map[string]interface{}
  iter := collection.Find(nil).Iter()
  for i := 0; i < 5 ; i++ {
    iter.Next(&result)
    err = updateFile(result)
    if err != nil {
      fmt.Println("Cannot update a mongo entry %v", err )
    }
  }
  if err := iter.Close(); err != nil {
    panic(err)
  }
}

func updateFile(book map[string]interface{}) error {
  b, err := json.MarshalIndent(book, "", " ")
  if err != nil {
    return err
  }
  err = ioutil.WriteFile(gitRepoPath + "/" + mongoCollection, b, 0644)
  if (err != nil) {
    return err
  }
  fmt.Printf("book: %s\n\n\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@", b)
  return nil
}

func commit() error {
  return nil
}

