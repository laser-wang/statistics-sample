package common

import (
	"github.com/garyburd/redigo/redis"
	"github.com/laser-wang/utils"
)

var (
	RedisConn    redis.Conn
	RedisDBIndex int
)

const (
	DataPath = "./data/test_data.csv"
)

type StatisticsResult struct {
	UserCnt int
	UserSum int
}

func ChkRepeat(key string, field string, redisConn redis.Conn) bool {
	ret, err := redisConn.Do("HGET", key, field)
	utils.CheckErr(err, utils.CHECK_FLAG_LOGONLY)
	if ret != nil {
		return true
	} else {
		retHsetNx, errHsetNx := redisConn.Do("HSETNX", key, field, "1")
		utils.CheckErrEcho(errHsetNx, utils.CHECK_FLAG_LOGONLY)
		retNx, _ := redis.Int(retHsetNx, errHsetNx)
		if retNx == 1 {
			return false
		} else {
			return true
		}
	}
}
