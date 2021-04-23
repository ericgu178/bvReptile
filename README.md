# <font color="green" style="text-align:center"> golang 哔哩哔哩爬取视频 </font>

## 借助了 goribot 爬取视频信息

## 请自行配置数据库 redis 默认本地 自行配置

`go mod tidy`

`go run main.go` 是 爬取了bvid 还有 插入数据库 奈何本人代理有限

`go run db.go` 是 将爬取到的bvid 插入或更新数据库后 标记 奈何本人代理有限

没有建立代理ip池 代码比较乱 **golang新手**

[推荐代理池](http://www.shenjidaili.com/product/open/)

扫码查看爬取后的排行榜单数据

![查看](./wx1.jpg)