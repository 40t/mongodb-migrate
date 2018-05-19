package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"fmt"
	"log"
	"sync"
	"math"
	"strconv"
	"strings"
)
const FETCH_SOURCE_LIMIT = 4000    //每次拉去数据条数
const CONCURRENCY_COUNT  = 10      //并发迁移任务数
const CHUNK_SIZE         = 400     //每次写入条数

var sourceUrl           string
var sourceCollectionStr string
var aimUrl              string
var aimCollectionStr    string
var sourceCollection    *mgo.Collection
var aimCollection       *mgo.Collection

func main() {
	Welcome()
	Configure()
	Migrate()
}

func Welcome() {
	fmt.Println("===================================================================================================================")
	fmt.Println("| Mongodb Migrate Tool")
	fmt.Println("===================================================================================================================")
	fmt.Println("| URL格式: mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]")
	fmt.Println("| URL例子[")
	fmt.Println("|     mongodb://127.0.0.1:27017/test")
	fmt.Println("|     mongodb://root:Vmjk9B@192.168.100.1:27017/entryDB")
	fmt.Println("| ]")
	fmt.Println("| 默认每次拉去条数: 4000")
	fmt.Println("| 默认并发迁移任务数: 100")
	fmt.Println("| 默认每个任务写入条数: 400")
	fmt.Println("===================================================================================================================")
}

func Configure()  {
	//source
	fmt.Println("(1/4) 请输入数据源URL:")
	fmt.Scanln(&sourceUrl)
	fmt.Println("(2/4) 请输入collection:")
	fmt.Scanln(&sourceCollectionStr)
	sourceCollectionStr = strings.TrimSpace(sourceCollectionStr)

	//aim
	fmt.Println("(3/4) 请输入目标源URL:")
	fmt.Scanln(&aimUrl)
	fmt.Println("(4/4) 请输入collection:")
	fmt.Scanln(&aimCollectionStr)
	aimCollectionStr = strings.TrimSpace(aimCollectionStr)
	fmt.Println("-------------------------------------------------------------------------------------------------------------------")
}

func Migrate() {
	_, sourceCollection = initDB(sourceUrl, sourceCollectionStr)
	_, aimCollection    = initDB(aimUrl, aimCollectionStr)

	var offset = 0
	var limit  = FETCH_SOURCE_LIMIT
	var waitGroup sync.WaitGroup
	totalResult,_ := sourceCollection.Count()
	//loop until no result of source's db
	for{
		//fetch data
		var sourceResult []interface{}
		err := sourceCollection.Find(bson.M{}).Skip(offset).Limit(limit).Sort("_id").All(&sourceResult)
		if err != nil {
			log.Fatal("error offset:", offset)
			break
		}
		if len(sourceResult) <= 0 {
			fmt.Println("已迁移完成")
			break
		}
		fmt.Println(offset,"/",totalResult,"("+strconv.Itoa(int(offset * 100 /totalResult))+"%"+")")
		offset = offset + limit

		//split data to chunk
		var chunkSize = CHUNK_SIZE
		chunk := mapChunk(sourceResult, chunkSize)

		//write data to aim db
		var pointer = 0
		var step = CONCURRENCY_COUNT
		l := len(chunk)
		for pointer < l {
			end := 0
			if (pointer + step) >= l {
				end = l
			} else {
				end = pointer + step
			}
			for pointer < end {
				waitGroup.Add(1)
				writeAimDB(&waitGroup, chunk[pointer])
				pointer++
			}
			waitGroup.Wait()
			pointer = end
		}
	}
}

func writeAimDB(waitGroup *sync.WaitGroup, data []interface{}) {
	bulk := aimCollection.Bulk()
	bulk.Insert(data...)
	_, err := bulk.Run()
	if err != nil {
		log.Print("Error in Insert", err.Error())
	}
	waitGroup.Done()
}

func mapChunk(raw []interface{}, size int) [][]interface{} {
	var result [][]interface{}
	mLen := len(raw)
	count := float64(mLen / size)
	count = math.Floor(count)
	c := int(count)
	if mLen <= size {
		return append(result, raw)
	}
	for i := 0; i < c; i++ {
		result = append(result,raw[i*size:i*size+size])
	}
	if (size * c) < mLen {
		result = append(result,raw[size * c:])
	}

	return result
}

func initDB(url string, c string) (*mgo.Session, *mgo.Collection){
	dialInfo,err := mgo.ParseURL(url)
	if err != nil {
		log.Fatal(err.Error())
	}

	server, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}
	server.SetMode(mgo.Monotonic, true)
	collection := server.DB(dialInfo.Database).C(c)

	return server, collection
}