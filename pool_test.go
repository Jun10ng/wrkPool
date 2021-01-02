package wrkPool

import "time"


func main() {
	opts := Options{Workers: 10}
	pool := New(opts)

	go func() {
		for r := range pool.ResultChan {
			_ = r
			time.Sleep(100 * time.Millisecond)
			//fmt.Println(r.(int))
		}
	}()
	for i := 0; i < 500; i++ {
		job := func(j int) (interface{}, error) {
			//fmt.Println(j)
			time.Sleep(100 * time.Millisecond)
			return j, nil
		}
		v := i
		pool.SubmitCheckResult(
			func() (interface{}, error) {
				return job(v)
			})
	}
	pool.WaitWrkStop(true)
}

