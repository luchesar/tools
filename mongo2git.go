package main

import (
  "fmt"
  "gopkg.in/mgo.v2"
  "os"
  "encoding/json"
//  "gopkg.in/mgo.v2/bson"
)

func main() {
  sess, err := mgo.Dial("mongodb://54.171.48.210/coinvision")
  if err != nil {
    fmt.Printf("Can't connect to mongo, go error %v\n", err)
    os.Exit(1)
  }
  defer sess.Close() 

  sess.SetSafe(&mgo.Safe{})
  collection := sess.DB("coinvision").C("bitfinex_l2")


  var result map[string]interface{}
  iter := collection.Find(nil).Iter()
  for i := 0; i < 5 ; i++ {
    iter.Next(&result)
    writeJson(result, i)
  }
  if err := iter.Close(); err != nil {
    panic(err)
  }

 /* result := make([]map[string]interface{}, 10)
  err = collection.Find(nil).All(&result)
  if err != nil {
    panic(err)
  }

  fmt.Printf("Result: %v", result)

  for r := range result {
    fmt.Printf("Result: %v\n", r)
  }*/
}

func writeJson(obj map[string]interface{}, index int) {
    b, err := json.Marshal(obj)
    if err != nil {
        panic(err)
    }
    fmt.Printf("book: %s\n\n\n", b)
}


