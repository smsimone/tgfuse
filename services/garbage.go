package services

import (
	"time"

	"it.smaso/tgfuse/configs"
	"it.smaso/tgfuse/tgfuse"
)

func StartGarbageCollector(rootNode *tgfuse.RootNode) {
	for {
		nodes := rootNode.Children()
		for name := range nodes {
			if node, ok := rootNode.Nodes[name]; ok {
				if node.File.ReadyToClean() {
					node.File.DeleteTmpFile()
				}
				if node.ReadyForCleanup() {
					node.ClearBuffers()
				}
			}
		}
		time.Sleep(time.Duration(configs.GC_DELAY) * time.Second)
	}
}
