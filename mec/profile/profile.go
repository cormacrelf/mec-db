package profile

import (
	"fmt"
	"time"
)

var funcs map[string][]time.Duration = make(map[string][]time.Duration)
var starts map[string]time.Time = make(map[string]time.Time)
var blips map[string][]time.Time = make(map[string][]time.Time)

func Trace(s string) (string, time.Time) {
	return s, time.Now()
}

func Un(s string, startTime time.Time) {
	endTime := time.Now()
	funcs[s] = append(funcs[s], endTime.Sub(startTime))
}

func Start(s string) {
	starts[s] = time.Now()
}

func End(s string) {
	if val,ok := starts[s]; ok {
		endTime := time.Now()
		funcs[s] = append(funcs[s], endTime.Sub(val))
		delete(starts, s)
	}
}

func Blip(s string) {
	blips[s] = append(blips[s], time.Now())
}

func avg(durs []time.Duration) time.Duration {
  total := float64(0)
  for _, d := range durs {
    total += d.Seconds()
  }
  return time.Duration((total / float64(len(durs))) * float64(time.Second))
}

func Print() {
	avgs := make(map[string]time.Duration)
	for k, v := range funcs {
		average := avg(v)
		avgs[k] = average
	}
	var acc []byte

	for k, dur := range avgs {
		acc = append(acc, []byte(fmt.Sprintf("%s: %v\n", k, dur))...)
	}

	fmt.Println(string(acc))

	for k, v := range blips {
		duration := v[len(v) - 1].Sub(v[0])
		bps := float64(len(v)) / duration.Seconds()
		fmt.Printf("%s: %v blips, %v bps", k, len(v), bps)
	}
}

func Reset() {
	funcs = make(map[string][]time.Duration)
}
