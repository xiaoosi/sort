package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)

var mytime time.Time

// Source data form a slice return a int chan
func SourceData(data ...int) <-chan int {
	fmt.Println("num:", len(data))
	ch := make(chan int, 80000000)
	go func() {
		for _, v := range data {
			ch <- v
		}
		close(ch)
	}()
	return ch
}

// Sort data
func SortLocal(in <-chan int) <-chan int {
	out := make(chan int, 80000000)
	go func() {
		var dataList []int
		for v := range in {
			dataList = append(dataList, v)
		}
		sort.Ints(dataList)
		fmt.Println("local sort over", time.Now().Sub(mytime))
		for _, v := range dataList {
			out <- v
		}
		//fmt.Println("local sort over", time.Now().Sub(mytime))
		close(out)
	}()
	return out
}

// Marge
func Marge(in1, in2 <-chan int) <-chan int {
	if in1 == nil {
		return in2
	}
	if in2 == nil {
		return in1
	}
	out := make(chan int, 80000000)
	go func() {
		d1, ok1 := <-in1
		d2, ok2 := <-in2
		for {
			if !ok1 && !ok2 {
				fmt.Println("marge over", time.Now().Sub(mytime))
				close(out)
				break
			} else if (ok1 && !ok2) || (ok1 && ok2 && (d1 < d2)) {
				out <- d1
				d1, ok1 = <-in1
			} else {
				out <- d2
				d2, ok2 = <-in2
			}
		}
	}()
	return out
}

func MargeAll(in ...<-chan int) <-chan int {
	length := len(in)
	if length == 1 {
		return in[0]
	}
	half := length / 2
	return Marge(MargeAll(in[:half]...), MargeAll(in[half:]...))
}

// Source random int number
func SourceRandom(length int) <-chan int {
	out := make(chan int, 80000000)
	go func() {
		for i := 0; i < length; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

func SourceReader(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 80000000)
	go func() {
		num := 0
		for {
			buffer := make([]byte, 8)
			n, err := reader.Read(buffer)
			if err != nil || (chunkSize != -1 && num >= chunkSize) {
				break
			}
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
				num += 8
			}
		}
		fmt.Println("read over", time.Now().Sub(mytime))
		close(out)
	}()
	return out
}

func WriterSink(write io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		_, _ = write.Write(buffer)
	}
}

func Init() {
	mytime = time.Now()
}
