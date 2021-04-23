package main

import (
	"crypto/tls"
	"context"
    "fmt"
	"io/ioutil"
	"encoding/json"
	"net/http"
	_ "strconv"
	"database/sql"
    "strings"
	_ "github.com/go-sql-driver/mysql"
	_ "time"
	"github.com/go-redis/redis/v8"
	"time"
	"net/url"
)


// 数据库配置

const (
    userName = ""
    password = ""
    ip = ""
    port = ""
    dbName = ""
)
var DB *sql.DB


var ctx = context.Background()

// redis 设置 key
func RedisSet(key string) bool {
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

    err1 := rdb.Set(ctx, key, key, 0).Err()
    if err1 != nil {
        panic(err1)
		return false
    }
	return true
}
// redis 设置 key
func RedisDel(key string) bool {
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

    err1 := rdb.Del(ctx, key).Err()
    if err1 != nil {
        panic(err1)
		return false
    }
	return true
}

// redis获取key

func RedisGet() bool {
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
	var cursor uint64
	for {
    	var keys []string
    	var err error
    	keys, cursor, err = rdb.Scan(ctx, cursor, "BV*", 0).Result()
    	if err != nil {
        	panic(err)
    	}

    	for _, key := range keys {
			fmt.Println("BVID", key)
			get(key)
			time.Sleep(0 *time.Second)
    	}

    	if cursor == 0 { // no more keys
        	break
    	}
	}
	return true
}

func get(BVID string) bool {
	var src = "https://api.bilibili.com/x/web-interface/view?bvid=" + BVID
	uri, err := url.Parse("http://150.138.253.70:808")
	fmt.Println(src)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy: http.ProxyURL(uri),
    }
    client := &http.Client{Transport: tr}	
	request, err := http.NewRequest("GET", src, nil)

    //增加header选项
    request.Header.Add("Cookie", "_uuid=794CE683-2B30-D9B8-7A77-DD2C3DA1199A65362infoc; buvid3=AAE72C27-F802-4DC5-8BAE-5B83BB1C239734777infoc; fingerprint=dd1a3808c3180a1fc36208cca5aa8815; buvid_fp=AAE72C27-F802-4DC5-8BAE-5B83BB1C239734777infoc; buvid_fp_plain=AAE72C27-F802-4DC5-8BAE-5B83BB1C239734777infoc; SESSDATA=f6bdc747%2C1634608183%2C302b2%2A41; bili_jct=8df41fb6bf70080c88b389c1b53ef169; DedeUserID=4210043; DedeUserID__ckMd5=c5d9f7754aeffdc7; sid=lqlrxm3h; CURRENT_FNVAL=80; blackside_state=1; rpdid=|(u||Y~R)JmY0J'uYum|~)umY; bfe_id=6f285c892d9d3c1f8f020adad8bed553")
    request.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36")
	data := map[string]interface{}{}

	response, err := client.Do(request)
	if err != nil {
		fmt.Println("1",err)
		return false
	}
    body, _ := ioutil.ReadAll(response.Body)
    json.Unmarshal(body, &data)

	defer response.Body.Close()

	res := map[string]interface{}{
		"bvid":  data["data"].(map[string]interface{})["bvid"],
		"tid":    data["data"].(map[string]interface{})["tid"],
		"title": data["data"].(map[string]interface{})["title"],
		"des":   data["data"].(map[string]interface{})["des"],
		"pic":   data["data"].(map[string]interface{})["pic"],
		"tname": data["data"].(map[string]interface{})["tname"],
		"mid":  data["data"].(map[string]interface{})["owner"].(map[string]interface{})["mid"],
		"name": data["data"].(map[string]interface{})["owner"].(map[string]interface{})["name"],
		"aid": 	data["data"].(map[string]interface{})["stat"].(map[string]interface{})["aid"],
		"ctime":   data["data"].(map[string]interface{})["ctime"],
		"pubdate": data["data"].(map[string]interface{})["pubdate"],
		"view":     data["data"].(map[string]interface{})["stat"].(map[string]interface{})["view"],
		"danmaku":  data["data"].(map[string]interface{})["stat"].(map[string]interface{})["danmaku"],
		"reply":    data["data"].(map[string]interface{})["stat"].(map[string]interface{})["reply"],
		"favorite": data["data"].(map[string]interface{})["stat"].(map[string]interface{})["favorite"],
		"coin":     data["data"].(map[string]interface{})["stat"].(map[string]interface{})["coin"],
		"share":    data["data"].(map[string]interface{})["stat"].(map[string]interface{})["share"],
		"like":     data["data"].(map[string]interface{})["stat"].(map[string]interface{})["like"],
		"dislike":  data["data"].(map[string]interface{})["stat"].(map[string]interface{})["dislike"],
	}
	fmt.Println(res)
		aid := data["data"].(map[string]interface{})["stat"].(map[string]interface{})["aid"]
		var name string
		errs := DB.QueryRow("SELECT name FROM bilibili_favorites WHERE aid = ?", aid).Scan(&name)
		if errs != nil{
			fmt.Println("查询出错了")
		}
		if name == "" {
			// 不存在
			fmt.Println("不存在添加")
			InsertBilibili(res)
		} else {
			fmt.Println("存在更新")
			UpdateBilibili(res)
		}
	RedisDel(BVID)
	RedisSet("_" + BVID)
	return true
}

func main() {
	InitDB()
	RedisGet()
}


// 链接数据库
func InitDB()  {
    //构建连接："用户名:密码@tcp(IP:端口)/数据库?charset=utf8"
    path := strings.Join([]string{userName, ":", password, "@tcp(",ip, ":", port, ")/", dbName, "?charset=utf8"}, "")

    //打开数据库,前者是驱动名，所以要导入： _ "github.com/go-sql-driver/mysql"
    DB, _ = sql.Open("mysql", path)
    //设置数据库最大连接数
    DB.SetConnMaxLifetime(100)
    //设置上数据库最大闲置连接数
    DB.SetMaxIdleConns(10)
    //验证连接
    if err := DB.Ping(); err != nil{
        fmt.Println("opon database fail")
        return
    }
    fmt.Println("connnect success")
}

/**
 * 添加 b站视频数据
 *
 */
func InsertBilibili(values map[string]interface{}) (bool){
	title := values["title"]
	pic := values["pic"].(string)
	created := values["pubdate"]
	aid := values["aid"]
	tid := values["tid"]
	favorite := values["favorite"]
	danmaku := values["danmaku"]
	reply := values["reply"]
	view := values["view"]
	share := values["share"]
	mid := values["mid"]
	name := values["name"]
	coin := values["coin"]
    //开启事务
    tx, err := DB.Begin()
    if err != nil{
        fmt.Println("tx fail",err)
        return false
    }
    //准备sql语句
    stmt, err := tx.Prepare("INSERT INTO bilibili_favorites (`title`, `pic` , `aid` , `copyright` , `typeid` , `playTotal` , `reviewTotal`, `danmakuTotal` , `favoritesTotal` , `created` , `mid` , `name`,`shareTotal`, `coinTotal`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    if err != nil{
		tx.Rollback()
        fmt.Println("Prepare fail",err)
        return false
    }
    //将参数传递到sql语句中并且执行
    res, err := stmt.Exec(title, pic[5:] , aid , "-1" , tid, view , reply , danmaku ,favorite  ,created , mid ,name , share , coin)
    if err != nil{
        fmt.Println("Exec fail",err)
		tx.Rollback()
        return false
    }
    //将事务提交
    err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return false
	}
    //获得上一个插入自增的id
    fmt.Println(res.LastInsertId())
    return true
}

/**
 * 添加 b站视频数据
 *
 */
 func UpdateBilibili(values map[string]interface{}) (bool){
	aid := values["aid"]
	favorite := values["favorite"]
	danmaku := values["danmaku"]
	reply := values["reply"]
	view := values["view"]
	share := values["share"]
	coin := values["coin"]
    //开启事务
    tx, err := DB.Begin()
    if err != nil{
		tx.Rollback()
        fmt.Println("tx fail",err)
        return false
    }
    //准备sql语句
    stmt, err := tx.Prepare("UPDATE bilibili_favorites SET playTotal=?, reviewTotal=?, danmakuTotal=?, favoritesTotal=?, shareTotal=?, coinTotal=? WHERE aid = ?")
    if err != nil{
        fmt.Println("Prepare fail",err)
        return false
    }
    //将参数传递到sql语句中并且执行
    res, err := stmt.Exec(view , reply , danmaku ,favorite, share , coin , aid)
    if err != nil{
        fmt.Println("Exec fail",err)
		tx.Rollback()
        return false
    }
    //将事务提交
    err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return false
	}
	fmt.Println(res.LastInsertId())
    return true
}