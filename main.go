package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/laser-wang/statistics-sample/common"
	"github.com/laser-wang/utils"
	"github.com/laser-wang/utils/cache"
)

func main() {
	defer releaseResource()
	setupRedis()

	//	makeTestData()

	result := statistics()
	log("user count:" + utils.Itoa(result.UserCnt))
	log("value sum:" + utils.Itoa(result.UserSum))

}

func statistics() *common.StatisticsResult {

	f, err := os.Open(common.DataPath)
	if err != nil {
		utils.CheckErrEcho(err, utils.CHECK_FLAG_EXIT)
	}
	defer f.Close()

	result := new(common.StatisticsResult)

	br := bufio.NewReader(f)
	for {
		lineByte, _, err := br.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			utils.CheckErrEcho(err, utils.CHECK_FLAG_EXIT)
		}
		line := string(lineByte)
		//		log(line)
		statisticsSub(line, result)
	}
	return result

}

func statisticsSub(line string, result *common.StatisticsResult) {
	lineSplit := strings.Split(line, ",")

	userId := lineSplit[0]
	value := lineSplit[1]

	if common.ChkRepeat(userId, "user_cnt", common.RedisConn) == false {
		result.UserCnt = result.UserCnt + 1
	}
	result.UserSum = result.UserSum + utils.Atoi(value)

}

func setupRedis() (err error) {

	url := "redis://@127.0.0.1:6379"
	maxIdles := 100
	idleTimeout := 240
	common.RedisDBIndex = 1

	cache.Init(url, maxIdles, idleTimeout, common.RedisDBIndex)
	err = cache.Ping()
	utils.CheckErrEcho(err, utils.CHECK_FLAG_EXIT)

	common.RedisConn = cache.Get()
	_, err = common.RedisConn.Do("select", common.RedisDBIndex)
	utils.CheckErrEcho(err, utils.CHECK_FLAG_EXIT)

	_, err = common.RedisConn.Do("flushdb")
	utils.CheckErrEcho(err, utils.CHECK_FLAG_EXIT)
	//	log("[redis db]:" + utils.Itoa(common.RedisDBIndex))

	return
}

func releaseResource() {
	if common.RedisConn != nil {
		common.RedisConn.Close()
	}
	cache.Close()

}

func log(msg string) {
	fmt.Println(msg)
}

func makeTestData() {

	rw := new(sync.RWMutex)

	f, err := os.Create(common.DataPath)
	if err != nil {
		utils.CheckErrEcho(err, utils.CHECK_FLAG_EXIT)
	}
	defer f.Close()

	hmUserId := make(map[int]string)
	hmUserId[0] = utils.GetMongoObjectId()
	hmUserId[1] = utils.GetMongoObjectId()
	hmUserId[2] = utils.GetMongoObjectId()
	hmUserId[3] = utils.GetMongoObjectId()
	hmUserId[4] = utils.GetMongoObjectId()

	for i := 0; i < 1000; i++ {
		rnd := utils.RandomInt(5)
		userId := hmUserId[rnd]
		makeTestDataSub(f, userId, rw)
	}
}

func makeTestDataSub(f *os.File, userId string, rw *sync.RWMutex) {

	rw.Lock()
	defer rw.Unlock()

	w := bufio.NewWriter(f)

	line := userId + "," + utils.Itoa(utils.RandomInt(1000)) + "\n"
	_, err := w.WriteString(line) //写入文件(字节数组)
	utils.CheckErrEcho(err, utils.CHECK_FLAG_EXIT)

	w.Flush()

}
