package main

import (
	"it.smaso/tgfuse/tgfuse"
	"log"
	"time"
)

func StartGarbageCollector(rootNode *tgfuse.RootNode) {
	for {
		nodes := rootNode.Children()
		for name := range nodes {
			if node, ok := rootNode.Nodes[name]; ok {
				if node.ReadyForCleanup() {
					node.ClearBuffers()
					log.Println("Cleared buffers of", name)
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}
