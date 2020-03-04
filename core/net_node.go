package pipeline

import (
	"bufio"
	"net"
)

func NetworkSource(address string)<-chan int {
	out := make(chan int, 8000000)
	go func() {
		conn, err := net.Dial("tcp", address)
		defer conn.Close()
		if err != nil {
			panic(err)
		}
		ch := SourceReader(bufio.NewReader(conn), -1)
		for v := range ch{
			out <- v
		}
		close(out)
	}()
	return out
}

func NetworkSink(address string, in <-chan int){
	go func() {
		ln, err := net.Listen("tcp", address)
		if err != nil {
			panic(err)
		}
		defer ln.Close()
		conn, err := ln.Accept()
		defer conn.Close()
		if err != nil {
			panic(err)
		}
		WriterSink(bufio.NewWriter(conn) , in)
	}()
}
