package requests

//import (
//	"github.com/VictoriaMetrics/metrics"
//	"github.com/gocolly/colly/v2"
//	"github.com/hugh2632/bloomfilter"
//	"sync"
//)
//
//type FormRequest struct {
//	Drive          string
//	Urls           string
//	Method         string
//	Connect        *colly.Collector
//	Process        func(*FormRequest, string, []string, string)
//	TasksChan      chan interface{}
//	TaskId         string
//	Company        string
//	NodeLimit      int32
//	NodeCount      int32
//	NodeCountNew   int32
//	NodeType       string
//	CloseOnce      sync.Once
//	Mutex          sync.Mutex
//	MutexC         sync.Mutex
//	Bloom          bloomfilter.IFilter
//	Done           bool
//	Domain         string
//	Root           string
//	RequestFailMts *metrics.Counter // 访问失败链接埋点计数器
//	RequestAllMts  *metrics.Counter // 访问链接总数埋点计数器
//}
//
//func (request *FormRequest) SetTasks(tasks chan interface{}) {
//	request.TasksChan = tasks
//}
//func (request *FormRequest) SetConnect(conn *colly.Collector) {
//	request.Connect = conn
//}
