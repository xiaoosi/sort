package main

import (
	"bufio"
	"os"
	"strconv"
	pipeline "xiaoosi.cn/pipline/core"
)

// 帅


func main() {
	//ch := pipeline.MargeAll(
	//	pipeline.SortLocal(pipeline.SourceData(8, 5, 6, 45, 8, 2, 3, 4, 7)),
	//	pipeline.SortLocal(pipeline.SourceData(9, 34, 156, 89, 489, 489, 45654, 78, 89)),
	//	pipeline.SortLocal(pipeline.SourceData(81, 45, 65, 45, 78, 28, 35, 47, 79)),
	//	pipeline.SortLocal(pipeline.SourceData(82222, 75, 68, 485,68, 2, 83, 4, 78)),
	//	pipeline.SortLocal(pipeline.SourceData(88, 55, 76, 457, 88, 52, 53, 4, 77)),
	//	pipeline.SortLocal(pipeline.SourceData(85, 85, 76, 458, 8, 27, 3, 74, 7)),
	//	pipeline.SortLocal(pipeline.SourceData(6, 8, 7, 4, 2, 4, 8, 8, 489, 1, 5, 6, 56, 45)))

	//
	//
	//ch := pipeline.SourceRandom(100000000)
	//file, err := os.Create("data.shc")
	//if err != nil {
	//	panic(err)
	//}
	//defer file.Close()
	//writer := bufio.NewWriter(file)
	//pipeline.WriterSink(writer , ch)
	//defer writer.Flush()

	pipeline.Init()

	fileSize := 800000000
	chunkNum := 4
	// 分段读取

	chunkSize := fileSize / chunkNum
	var chs []<-chan int
	var addrs []string
	for i := 0; i < chunkNum; i++ {
		file, err := os.Open("data.shc")
		if err != nil {
			panic(err)
		}
		defer file.Close()
		file.Seek(int64(i*chunkSize), 0)
		ch := pipeline.SourceReader(bufio.NewReader(file), chunkSize)
		ch = pipeline.SortLocal(ch)
		address := ":" + strconv.Itoa(7000 + i)
		pipeline.NetworkSink(address, ch)
		addrs = append(addrs, address)
	}


	for _, address := range addrs {
		ch := pipeline.NetworkSource(address)
		chs = append(chs, ch)
	}

	out := pipeline.MargeAll(chs...)

	file, err := os.Create("out.shc")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(bufio.NewWriter(file))
	defer writer.Flush()
	pipeline.WriterSink(writer, out)
}
