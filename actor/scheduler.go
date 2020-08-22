package actor

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type CancelFunc func()

const (
	stateInit = iota
	stateReady
	stateDone
)

var DefaultScheduler = &timerScheduler{}

func startTimer(delay, interval time.Duration, fn func()) CancelFunc {
	var t *time.Timer
	var state int32
	t = time.AfterFunc(delay, func() {
		state := atomic.LoadInt32(&state)
		if state == stateInit {
			runtime.Gosched()
			state = atomic.LoadInt32(&state)
		}

		if state == stateDone {
			return
		}

		fn()
		t.Reset(interval)
	})
	atomic.StoreInt32(&state, stateReady)

	return func() {
		if atomic.SwapInt32(&state, stateDone) != stateDone {
			t.Stop()
		}
	}
}

type timerScheduler struct {
}

func (s *timerScheduler) sendOnce(delay time.Duration, ref *Ref, message interface{}) CancelFunc {
	t := time.AfterFunc(delay, func() {
		err := ref.pushMsg(message)
		if err != nil {
			logrus.WithError(err).Errorf("Scheduler SendOnce Run Fail")
		}
	})
	return func() {
		t.Stop()
	}
}

func (s *timerScheduler) sendRepeatedly(initial, interval time.Duration, ref *Ref, message interface{}) CancelFunc {
	cancel := startTimer(initial, interval, func() {
		err := ref.pushMsg(message)
		if err != nil {
			logrus.WithError(err).Errorf("Scheduler SendRepeatedly Run Fail")
		}

	})
	return cancel
}

func SendOnce(delay time.Duration, ref *Ref, message interface{}) CancelFunc {
	return DefaultScheduler.sendOnce(delay, ref, message)
}

func SendRepeatedly(initial, interval time.Duration, ref *Ref, message interface{}) CancelFunc {
	return DefaultScheduler.sendRepeatedly(initial, interval, ref, message)
}
