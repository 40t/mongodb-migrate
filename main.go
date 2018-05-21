package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"math"
	"time"
)

const FetchSourceLimit        = 5000      //每次拉去数据条数
const SplitFetchSourceLimit   = 500       //每个拉取任务一次拉去的数据
const WorkerCount             = 50        //worker数量

var sourceUrl           string
var sourceCollectionStr string
var aimUrl              string
var aimCollectionStr    string
var sourceCollection    *mgo.Collection
var aimCollection       *mgo.Collection

func main() {
	now := time.Now().Unix()
	Welcome()
	Configure()
	Migrate()
	ms := time.Now().Unix() - now
	fmt.Println("耗时:"+strconv.Itoa(int(ms))+"s")
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
//  创建带缓冲的 channel
var ch = make(chan []interface{}, 1000)
func Migrate() {
	_, sourceCollection = initDB(sourceUrl, sourceCollectionStr)
	_, aimCollection    = initDB(aimUrl, aimCollectionStr)

	//  运行固定数量的 workers
	for i := 0; i < WorkerCount; i++ {
		go worker(ch)
	}

	//  发送任务到 workers
	var offset int = 0
	var limit  int = FetchSourceLimit
	totalResult,_ := sourceCollection.Count()
	for {
		var waitGroup sync.WaitGroup
		offsetRange := splitOffsetRange(offset, limit)
		for offset, limit := range offsetRange {
			waitGroup.Add(1)
			go getTask(offset, limit, &waitGroup)
		}
		waitGroup.Wait()

		offset = offset + limit
		fmt.Println(offset,"/",totalResult,"("+strconv.Itoa(int(offset * 100 /totalResult))+"%"+")")

		if offset >= totalResult {
			fmt.Println("已经完成")
			break
		}
	}

}

func splitOffsetRange(offset int, limit int) map[int]int {
	var result = make(map[int]int)
	if (FetchSourceLimit <= SplitFetchSourceLimit) {
		result[offset] = limit
		return  result
	}

	var count = float64(FetchSourceLimit / SplitFetchSourceLimit)
	c := int(math.Floor(count))

	for i := 0; i < c; i++{
		result[offset + i*SplitFetchSourceLimit] = SplitFetchSourceLimit
	}

	if SplitFetchSourceLimit * c < FetchSourceLimit {
		hasFilledCount := SplitFetchSourceLimit * c
		result[hasFilledCount] = FetchSourceLimit - hasFilledCount
	}

	return result
}

func getTask(offset int, limit int, group *sync.WaitGroup) bool {
	//fetch data
	var sourceResult []interface{}
	err := sourceCollection.Find(bson.M{}).Skip(offset).Limit(limit).Sort("_id").All(&sourceResult)
	if err != nil {
		log.Fatal("error offset:", offset)
	}
	if len(sourceResult) <= 0 {
		group.Done()
		return  false
	}
	ch <- sourceResult
	group.Done()
	return true
}

func worker(ch chan []interface{}) {
	for {
		//接收任务
		task := <-ch
		writeAimDB(task)
	}
}

func writeAimDB( data []interface{}) {
	bulk := aimCollection.Bulk()
	bulk.Insert(data...)
	_, err := bulk.Run()
	if err != nil {
		log.Print("Error in Insert", err.Error())
	}
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

	collection := server.DB(dialInfo.Database).C(c)

	return server, collection
}