package syncutil

import (
	"sync/atomic"
)

// shutdown signaling for goroutines
var finishFlag uint32 = 0

// signals service shutdown
func SignalServiceShutdown() {
	atomic.StoreUint32(&finishFlag, 1)
}

// interested goroutines call this function to check if the service is being shut down
func SignaledServiceShutdown() bool {
	if atomic.LoadUint32(&finishFlag) == 1 {
		return true
	}
	return false
}
