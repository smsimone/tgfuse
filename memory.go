package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

func StartMemoryChecker() {
	log.Println("Starting memory checker")
	file, err := os.OpenFile("/Users/antlia/Development/tgfuse_go/stats.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	_, _ = file.WriteString("time,heap alloc,heap sys,heap in use,total alloc\n")
	_ = file.Close()

	for {
		stats := &runtime.MemStats{}
		runtime.ReadMemStats(stats)
		writeToFile(stats)
		time.Sleep(1 * time.Second)
	}
}

func writeToFile(stats *runtime.MemStats) {
	file, _ := os.OpenFile("/Users/antlia/Development/tgfuse_go/stats.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	defer file.Close()
	_, _ = file.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d\n", time.Now().UnixMilli(), stats.HeapAlloc, stats.HeapSys, stats.HeapInuse, stats.TotalAlloc))
}
