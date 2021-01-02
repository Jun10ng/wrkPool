package wrkPool

import (
	"sync"
	"sync/atomic"
)

const (
	// 默认worker队列大小
	defaultQSize = 128
	// 输出队列大小
	outputChanSize = 100
)

type Workers struct {
	numWorkers uint32
	maxWorkers uint32
	numJobs    uint32
	workerQ    chan func()
	bufferedQ  chan func()
	jobQ       chan func()
	stopping   int32
	done       chan struct{}

	// ErrChan 收集job产生的err，在Stop() return后被关闭。
	// 仅对SubmitCheckError()和SubmitCheckResult()有效
	// 在submit job之前监听ErrChan，以便不会漏过任何更新
	ErrChan chan error

	ResultChan chan interface{}
}

// Workers worker数目，如果没指定或者0，则按需求生成worker
// QSize job数
// Minimum value is 128.
type Options struct {
	Workers uint32
	QSize   uint32
}

func New(args ...Options) *Workers {
	wrk := &Workers{
		workerQ: make(chan func()),
		// Do not remove jobQ. To stop receiving input once WaitWrkStop() is called
		jobQ:       make(chan func()),
		ErrChan:    make(chan error, outputChanSize),
		ResultChan: make(chan interface{}, outputChanSize),
		done:       make(chan struct{}),
	}

	wrk.bufferedQ = make(chan func(), defaultQSize)
	if len(args) == 1 {
		wrk.maxWorkers = args[0].Workers
		if args[0].QSize > defaultQSize {
			wrk.bufferedQ = make(chan func(), args[0].QSize)
		}
	}

	go wrk.start()

	return wrk
}

func (wrk *Workers) JobNum() uint32 {
	return atomic.LoadUint32(&wrk.numJobs)
}

func (wrk *Workers) WorkerNum() uint32 {
	return atomic.LoadUint32(&wrk.numWorkers)
}

// 无返回error的job
func (wrk *Workers) Submit(job func()) {
	if atomic.LoadInt32(&wrk.stopping) == 1 {
		return
	}
	atomic.AddUint32(&wrk.numJobs, uint32(1))
	wrk.jobQ <- func() { job() }
}

// 有返回error的job
func (wrk *Workers) SubmitCheckError(job func() error) {
	if atomic.LoadInt32(&wrk.stopping) == 1 {
		return
	}
	atomic.AddUint32(&wrk.numJobs, uint32(1))
	wrk.jobQ <- func() {
		err := job()
		if err != nil {
			select {
			case wrk.ErrChan <- err:
			default:
			}
		}
	}
}

// 需要结果与错误
func (wrk *Workers) SubmitCheckResult(job func() (interface{}, error)) {
	if atomic.LoadInt32(&wrk.stopping) == 1 {
		return
	}
	atomic.AddUint32(&wrk.numJobs, uint32(1))
	wrk.jobQ <- func() {
		result, err := job()
		if err != nil {
			select {
			case wrk.ErrChan <- err:
			default:
			}
		} else {
			select {
			case wrk.ResultChan <- result:
			default:
			}
		}
	}
}

// waitChannel = true 会等待errChan 或者 resultChan 消费干净
// 所以当你想接受所有的result或者err时 请使用true
func (wrk *Workers) WaitWrkStop(waitChannel bool) {
	if !atomic.CompareAndSwapInt32(&wrk.stopping, 0, 1) {
		return
	}
	if wrk.JobNum() != 0 {
		<-wrk.done
	}

	if waitChannel {
		for len(wrk.ResultChan)|len(wrk.ErrChan) == 0 {
			break
		}
	}

	// close the input channel
	close(wrk.jobQ)
}

var mx sync.Mutex

// 判断是否需要生成新的worker
func (wrk *Workers) genWoker() {
	defer mx.Unlock()
	mx.Lock()
	if ((wrk.maxWorkers == 0) || (wrk.WorkerNum() < wrk.maxWorkers)) && (wrk.JobNum() > wrk.WorkerNum()) {
		go wrk.startWorker()
	}
}

func (wrk *Workers) start() {
	defer func() {
		close(wrk.bufferedQ)
		close(wrk.workerQ)
		close(wrk.ErrChan)
		close(wrk.ResultChan)
	}()

	// start a worker in advance
	go wrk.startWorker()

	go func() {
		for {
			select {
			// keep processing the queued jobs
			case job, ok := <-wrk.bufferedQ:
				if !ok {
					return
				}
				go func() {
					wrk.genWoker()
					wrk.workerQ <- job
				}()
			}
		}
	}()

	for {
		select {
		case job, ok := <-wrk.jobQ:
			if !ok {
				return
			}
			select {
			// if possible, process the job without queueing
			case wrk.workerQ <- job:
				go wrk.genWoker()
			// queue it if no workers are available
			default:
				wrk.bufferedQ <- job
			}
		}
	}
}

// 实际处理步骤，没调用一次startWorker都相当于启动一个协程
func (wrk *Workers) startWorker() {
	defer func() {
		atomic.AddUint32(&wrk.numWorkers, ^uint32(0))
	}()

	atomic.AddUint32(&wrk.numWorkers, 1)

	for job := range wrk.workerQ {
		// 不断接收任务处理
		job()
		if (atomic.AddUint32(&wrk.numJobs, ^uint32(0)) == 0) && (atomic.LoadInt32(&wrk.stopping) == 1) {
			wrk.done <- struct{}{}
		}
	}
}
