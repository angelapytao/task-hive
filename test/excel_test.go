package test

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/tealeg/xlsx"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExcel(t *testing.T) {
	// 获取当前文件所在目录
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(filename))
	excelPath := filepath.Join(projectRoot, "spider.xlsx")

	// 读取excel任务数据
	xlFile, err := xlsx.OpenFile(excelPath)
	if err != nil {
		logrus.Errorf("startWechatTask Error opening Excel file: %v", err)
		return
	}

	if len(xlFile.Sheets) == 0 {
		logrus.Error("Excel file has no sheets")
		return
	}

	sheet := xlFile.Sheets[0]
	fmt.Println(sheet.Name)
	fmt.Println(len(sheet.Rows))
	//// 跳过表头
	//for rowIndex, row := range sheet.Rows[1:] {
	//	for colIndex, cell := range row.Cells {
	//	}
	//}
}
