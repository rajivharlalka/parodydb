package notif

import (
	"sync"
	"time"
)

type Notifier struct {
	ch    chan struct{}
	wg    sync.WaitGroup
	mutex sync.Mutex
}

func NewNotifier() *Notifier {
	return &Notifier{ch: make(chan struct{})}
}

func (n *Notifier) Wait(maxTimeToWait time.Duration) {
	n.wg.Add(1)
	go func() {
		select {
		case <-n.ch:

		case <-time.After(maxTimeToWait):

		}
		n.wg.Done()
	}()
}

func (n *Notifier) NotifyAll() {
	n.mutex.Lock()
	close(n.ch)
	n.ch = make(chan struct{})
	n.mutex.Unlock()
	n.wg.Wait()
}
