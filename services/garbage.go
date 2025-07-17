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
				if node.ReadyForCleanup() {
					node.ClearBuffers()
					// logger.LogInfo(fmt.Sprintf("Cleared buffers of %s", name))
				}
			}
		}
		// runtime.GC()
		time.Sleep(time.Duration(configs.GC_DELAY) * time.Second)
	}
}
