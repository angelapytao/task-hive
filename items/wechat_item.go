package items

//import "repost-spider/common"
//
//// GetJobDataCountResp 获取数说任务数据条数返回结构体
//type GetJobDataCountResp struct {
//	Message *string `json:"message"` // 使用指针类型，因为 message 可能是 null
//	Success bool    `json:"success"`
//	Code    int     `json:"code"`
//	Data    struct {
//		Total int `json:"total"`
//	} `json:"data"`
//}
//
//// GetJobDataDetailItem 获取数说任务数据详情返回结构体，字段释义：https://dc.datastory.com.cn/#/wiki
//// ***后续需要转存其他字段的时候再放开对应的字段***
//type GetJobDataDetailItem struct {
//	//Sentiment      int      `json:"sentiment"`
//	//InteractionCnt int      `json:"interaction_cnt"`
//	//ReviewCnt      int      `json:"review_cnt"`
//	//ViewCnt        int      `json:"view_cnt"`
//	//Keywords       []string `json:"keywords"`
//	//Mid            string   `json:"mid"`
//	//Wxid           string   `json:"wxid"`
//	Title string `json:"title"`
//	//ProfilersName  string   `json:"profilers_name"`
//	//Content        string   `json:"content"`
//	Uid string `json:"uid"`
//	//UpdateTime     int64    `json:"update_time"`
//	PublishTime int64 `json:"publish_time"`
//	//PostRegion     string   `json:"post_region"`
//	//CatID          int      `json:"cat_id"`
//	ID string `json:"id"`
//	//Brief          string   `json:"brief"`
//	//LikeShareCnt   int      `json:"like_share_cnt"`
//	//IsOriginal     string   `json:"is_original"`
//	//IsAd           string   `json:"is_ad"`
//	//Author         string   `json:"author"`
//	PicUrls []string `json:"pic_urls"`
//	Url     string   `json:"url"`
//	//RepostsCnt     int      `json:"reposts_cnt"`
//	//ProfilersID    string   `json:"profilers_id"`
//	//SiteName       string   `json:"site_name"`
//	//IsMainPost     string   `json:"is_main_post"`
//	//ContentMode    string   `json:"content_mode"`
//	DataType string `json:"data_type"`
//	//SiteID         int      `json:"site_id"`
//	//Idx            string   `json:"idx"`
//	//LikeCnt        int      `json:"like_cnt"`
//	//Desc           string   `json:"desc"`
//}
//
//// GetJobDataDetailResp 获取数说任务数据详情
//type GetJobDataDetailResp struct {
//	Message *string `json:"message"` // 消息字段，可能为 null
//	Success bool    `json:"success"` // 请求是否成功
//	Code    int     `json:"code"`    // 状态码
//	Data    struct {
//		Doc             []GetJobDataDetailItem `json:"docs"`            //数据详情
//		LastPublishTime int64                  `json:"lastPublishTime"` // 最后发布时间，时间戳（毫秒）
//		ScrollCount     int                    `json:"scrollCount"`     // 滚动计数
//		ScrollId        string                 `json:"scrollId"`        // 滚动 ID
//	} `json:"data"` // 数据字段
//}
//
//type DataStoryData struct {
//	ID          int    `gorm:"column:id;primary_key;AUTO_INCREMENT"`       // 自增ID
//	DataType    string `gorm:"column:data_type;type:varchar(64);not null"` // 数据类型
//	UID         string `gorm:"column:uid;type:varchar(64);not null"`       // 用户ID
//	Title       string `gorm:"column:title;type:varchar(255);not null"`    // 标题
//	PublishTime int64  `gorm:"column:publish_time;type:bigint;not null"`   // 发布时间
//	PicURLs     string `gorm:"column:pic_urls;type:json;not null"`         // 图片URL数组
//	URL         string `gorm:"column:url;type:varchar(255);not null"`      // 文章URL
//}
//
//func (c DataStoryData) TableName() string {
//	return "datastory_data"
//}
//
//func DataStorySaveItem(item *DataStoryData) error {
//	return common.DB.Create(item).Error
//}
//
//func DataStoryItemCount() (int64, error) {
//	var count int64
//	err := common.DB.Model(&DataStoryData{}).Count(&count).Error
//	return count, err
//}
