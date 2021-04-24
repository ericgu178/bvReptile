package main

import (
	"context"
    "fmt"
	"database/sql"
    "github.com/PuerkitoBio/goquery"
    "github.com/zhshch2002/goribot"
    "strings"
	_ "github.com/go-sql-driver/mysql"
	_ "time"
	"github.com/go-redis/redis/v8"
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
        fmt.Println(err1)
		return false
    }
	return true
}

// redis获取key

func RedisGet(key string) bool {
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

	_, err := rdb.Get(ctx, "_" + key).Result()
	_, err2 := rdb.Get(ctx, key).Result()

    if err == redis.Nil && err2 == redis.Nil {
        RedisSet(key)
		return false
    } else {
		return true
    }
}


func main() {
	InitDB()

    s := goribot.NewSpider(
        goribot.Limiter(true, &goribot.LimitRule{
            Glob: "*.bilibili.com",
            Rate: 5,
			// RandomDelay: 5 * time.Second,// 随机间隔延时（同 host 下每个请求间隔 [0,5) 秒）
        }),
		goribot.SpiderLogPrint(),
		goribot.RefererFiller(),
        goribot.SetDepthFirst(true),
		goribot.ReqDeduplicate(),
		goribot.RandomUserAgent(),
		// goribot.RandomProxy(
		// 	"http://183.213.26.12:3128",
		// ),
    )
    var getVideoInfo = func(ctx *goribot.Context) {
        res := map[string]interface{}{
            "bvid":  ctx.Resp.Json("data.bvid").String(),
			"tid":    ctx.Resp.Json("data.tid").String(),
            "title": ctx.Resp.Json("data.title").String(),
            "des":   ctx.Resp.Json("data.des").String(),
            "pic":   ctx.Resp.Json("data.pic").String(),   // 封面图
            "tname": ctx.Resp.Json("data.tname").String(), // 分类名
			"mid":  ctx.Resp.Json("data.owner.mid").String(),
			"name": ctx.Resp.Json("data.owner.name").String(),
			"aid": 	ctx.Resp.Json("data.stat.aid").String(),
            "ctime":   ctx.Resp.Json("data.ctime").String(), // 创建时间
            "pubdate": ctx.Resp.Json("data.pubdate").String(), // 发布时间
            "view":     ctx.Resp.Json("data.stat.view").Int(),
            "danmaku":  ctx.Resp.Json("data.stat.danmaku").Int(),
            "reply":    ctx.Resp.Json("data.stat.reply").Int(),
            "favorite": ctx.Resp.Json("data.stat.favorite").Int(),
            "coin":     ctx.Resp.Json("data.stat.coin").Int(),
            "share":    ctx.Resp.Json("data.stat.share").Int(),
            "like":     ctx.Resp.Json("data.stat.like").Int(),
            "dislike":  ctx.Resp.Json("data.stat.dislike").Int(),
        }
		fmt.Println(res)
        ctx.AddItem(res)
    }
    var findVideo goribot.CtxHandlerFun
    findVideo = func(ctx *goribot.Context) {
        u := ctx.Req.URL.String()
        // fmt.Println(u)
        if strings.HasPrefix(u, "https://www.bilibili.com/video/") {
            if strings.Contains(u, "?") {
                u = u[:strings.Index(u, "?")]
            }
            u = u[31:43] // 截取字符串
			if !RedisGet(u) {
				fmt.Println(u)
				ctx.AddTask(goribot.GetReq("https://api.bilibili.com/x/web-interface/view?bvid="+u), getVideoInfo)
			}
        }
        ctx.Resp.Dom.Find("a[href]").Each(func(i int, sel *goquery.Selection) {
            if h, ok := sel.Attr("href"); ok {
				if strings.HasPrefix(h, "/video") {
					ctx.AddTask(goribot.GetReq(h), findVideo)
				}
            }
        })
    }
    s.OnItem(func(m interface{}) interface{} {
		var data map[string]interface{} = m.(map[string]interface{})
		aid := data["aid"]
		var name string
		err := DB.QueryRow("SELECT name FROM bilibili_favorites WHERE aid = ?", aid).Scan(&name)
		if err != nil{
			fmt.Println("查询出错了")
		}
		if name == "" {
			// 不存在
			fmt.Println("不存在添加")
			InsertBilibili(data)
		} else {
			fmt.Println("存在更新")
			UpdateBilibili(data)
		}
        return m
    })

	s.OnError(func(ctx *goribot.Context, err error){
		fmt.Println("错误发生了")
		fmt.Println(err)
	}) 

    s.AddTask(goribot.GetReq("https://www.bilibili.com/video/BV1q64y1y7e2").SetHeader("cookie", "_uuid=1B9F036F-8652-DCDD-D67E-54603D58A9B904750infoc; buvid3=5D62519D-8AB5-449B-A4CF-72D17C3DFB87155806infoc; sid=9h5nzg2a; LIVE_BUVID=AUTO7815811574205505; CURRENT_FNVAL=16; im_notify_type_403928979=0; rpdid=|(k|~uu|lu||0J'ul)ukk)~kY; _ga=GA1.2.533428114.1584175871; PVID=1; DedeUserID=403928979; DedeUserID__ckMd5=08363945687b3545; SESSDATA=b4f022fe%2C1601298276%2C1cf0c*41; bili_jct=2f00b7d205a97aa2ec1475f93bfcb1a3; bp_t_offset_403928979=375484225910036050"), findVideo)
    s.Run()
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