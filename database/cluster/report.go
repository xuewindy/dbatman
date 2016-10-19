package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"time"

	"github.com/ngaut/log"
)

// "code.byted.org/gopkg/metrics"

type EventItem struct {
	Goroutines     string
	GCFreq         string
	TotalAllocated string
	Mallocs        string
	Frees          string
	HeapObjects    string
	// GCCPUFraction  float64
	// GCPauseNs      [256]uint64
	Heap  string
	Stack string
	NumGC string
}
type Stats struct {
	memStats *runtime.MemStats
}

func NewStats() *Stats {
	s := &Stats{
		memStats: &runtime.MemStats{},
	}
	runtime.ReadMemStats(s.memStats)
	return s
}

func Goroutines(e EventItem) interface{} {
	return e.Goroutines
}

// Allocated means still be used memory.
func TotalAllocated(e EventItem) interface{} {
	return e.TotalAllocated
}

// Mallocs return number of mallocs does.
func Mallocs(e EventItem) interface{} {
	return e.Mallocs
}

// Frees return number of free does.
func Frees(e EventItem) interface{} {
	return e.Frees
}

// HeapObject return number of object allocated
func HeapObject(e EventItem) interface{} {
	return e.HeapObjects
}

// GCCPUFraction
// func GCCPUFraction(e EventItem) interface{} {
// 	return e.GCCPUFraction
// }

// GCPause return pause time
// func GCPause(e EventItem) interface{} {
// 	var (
// 		totalPauseN int64 = 0
// 		sampleN           = 5
// 		n                 = 5
// 	)

// 	for n > 0 {
// 		totalPauseN += int64(e.GCPauseNs[(int(e.NumGC)+255-n)%256])
// 		n--
// 	}
// 	return int64(totalPauseN / 1000 / int64(sampleN))
// }

// Heap return size of heap.
func Heap(e EventItem) interface{} {
	return e.Heap
}

// Stack return size of stack.
func Stack(e EventItem) interface{} {
	return e.Stack
}
func toString(data interface{}) string {
	var str string
	switch da := data.(type) {
	case int:
		str = strconv.Itoa(da)
	case int64:
		str = strconv.FormatInt(da, 10)
	}
	return str

}
func (s *Stats) Event() <-chan EventItem {
	ret := make(chan EventItem)
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for _ = range ticker.C {
			numGcs := s.memStats.NumGC
			runtime.ReadMemStats(s.memStats)
			ret <- EventItem{
				Goroutines:     toString(int64(runtime.NumGoroutine())),
				GCFreq:         toString(int64(s.memStats.NumGC - numGcs)),
				TotalAllocated: toString(int64(s.memStats.TotalAlloc)),
				Mallocs:        toString(int64(s.memStats.Mallocs)),
				Frees:          toString(int64(s.memStats.Frees)),
				HeapObjects:    toString(int64(s.memStats.HeapObjects)),
				// GCCPUFraction:  s.memStats.GCCPUFraction,
				// GCPauseNs:      s.memStats.PauseNs,
				Heap:  toString(int64(s.memStats.HeapAlloc)),
				Stack: toString(int64(s.memStats.StackInuse)),
				NumGC: toString(int64(s.memStats.NumGC)),
			}
		}
	}()

	return ret
}

// NumGc return number of garbage collections every second.
func NumGc(e EventItem) interface{} {
	return e.GCFreq
}

type Reproter struct {
	name string
}

// type ReportValue func(e EventItem) string{}

func (r *Reproter) Reporting(addr string) {

	var reportData map[string]string
	states := NewStats()
	// for true {
	for e := range states.Event() {
		reportData = map[string]string{
			r.name + ".heap.byte":           e.Heap,
			r.name + ".stack.byte":          e.Stack,
			r.name + ".numGcs":              e.NumGC,
			r.name + ".numGos":              e.Goroutines,
			r.name + ".malloc":              e.Mallocs,
			r.name + ".free":                e.Frees,
			r.name + ".totalAllocated.byte": e.TotalAllocated,
			r.name + ".objects":             e.HeapObjects,
		}
		for k, v := range reportData {
			fmt.Printf("key :%s,value :%v\n", k, v)
		}
		data, _ := json.Marshal(reportData)
		transferReport(addr, data)
	}

}
func transferReport(addr string, data []byte) {
	conn, err := net.Dial("udp", addr)
	defer conn.Close()
	if err != nil {
		log.Warn("Open report addr err return")
		return
	}

	conn.Write([]byte(data))

}
