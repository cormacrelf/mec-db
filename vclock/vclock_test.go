package vclock

import "testing"

func TestEqual(t *testing.T) {
	// Different timestamps, same values
	A := VClock{"lion": Entry{counter: 1309, timestamp: 1389503545254049010}, "gazelle": Entry{counter: 1, timestamp: 1389503545254049981}}
	B := VClock{"lion": Entry{counter: 1309, timestamp: 1389503545254050111}, "gazelle": Entry{counter: 1, timestamp: 1389503545254050391}}

	if !Equal(A, B) {
		defer t.Error("Different timestamps should be equal")
	}

	C, D := Fresh(), Fresh()

	if !Equal(C, D) {
		defer t.Error("Empty VClocks should be equal")
	}
}

func TestValidationShort(t *testing.T) {
	A := VClock{"lion": Entry{counter: 10, timestamp: -5}}
	B := VClock{"lion": Entry{counter: -10, timestamp: 1389504412525473176}}
	C := VClock{"": Entry{counter: 10, timestamp: 1389504412525473176}}
	D := VClock{"lion": Entry{counter: 10, timestamp: 1389504412525473176}}
	a, b, c, d := A.IsValid(), B.IsValid(), C.IsValid(), D.IsValid()
	switch {
	case a:
		t.Error("validated negative timestamp")
	case b:
		t.Error("validated negative counter")
	case c:
		t.Error("validated empty client id")
	case !d:
		t.Error("invalidated correct VClock")
	}
}

func TestMerge(t *testing.T) {
	A := VClock{"lion": Entry{counter: 1309, timestamp: 1}, "gazelle": Entry{counter: 1, timestamp: 13}, "zebra": Entry{counter: 7, timestamp: 9}}
	B := VClock{"lion": Entry{counter: 1, timestamp: 138}, "gazelle": Entry{counter: 6, timestamp: 1389}}

	C := Merge([]VClock{A, B})
	D := VClock{"lion":Entry{counter:1309, timestamp:138}, "gazelle":Entry{counter:6, timestamp:1389}, "zebra":Entry{counter:7, timestamp:9}}

	// we want timestamps to merge too.
	if !Equal(C, D) {
		t.Error("merge failed:\n", C, "!=", D)
	}
}

func TestCompare(t *testing.T) {
	// stub
}

// Benchmarks

func BenchmarkMerge(b *testing.B) {
	A := VClock{"lion": Entry{counter: 1309, timestamp: 1}, "gazelle": Entry{counter: 1, timestamp: 13}, "zebra": Entry{counter: 7, timestamp: 9}}
	B := VClock{"lion": Entry{counter: 1, timestamp: 138}, "gazelle": Entry{counter: 6, timestamp: 1389}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Merge([]VClock{A, B})
	}
}

func BenchmarkCompare(b *testing.B) {
	A := VClock{"lion": Entry{counter: 1309, timestamp: 1}, "gazelle": Entry{counter: 1, timestamp: 13}, "zebra": Entry{counter: 7, timestamp: 9}}
	B := VClock{"lion": Entry{counter: 1, timestamp: 138}, "gazelle": Entry{counter: 6, timestamp: 1389}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Compare(A, B)
	}
}

func BenchmarkEqual(b *testing.B) {
	A := VClock{"lion": Entry{counter: 1309, timestamp: 1}, "gazelle": Entry{counter: 1, timestamp: 13}, "zebra": Entry{counter: 7, timestamp: 9}}
	B := VClock{"lion": Entry{counter: 1, timestamp: 138}, "gazelle": Entry{counter: 6, timestamp: 1389}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Equal(A, B)
	}
}

func BenchmarkIncrement(b *testing.B) {
	A := VClock{"lion": Entry{counter: 1309, timestamp: 1}, "gazelle": Entry{counter: 1, timestamp: 13}, "zebra": Entry{counter: 7, timestamp: 9}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		A.Increment("gazelle")
	}
}

// This section is a bit of fun, using panics, defers and goroutines to check
// 4 bools.

func validate(vc VClock) {
	if !vc.IsValid() {
		panic("invalid clock")
	}
}

func rec(vc VClock, mq chan bool) {
	defer func() {
		if r := recover(); r != nil {
			mq <- false
		} else {
			mq <- true
		}
	}()
	validate(vc)
}

func doNotTestValidation(t *testing.T) {
	A := VClock{"lion": Entry{counter: 10, timestamp: -5}}
	B := VClock{"lion": Entry{counter: -10, timestamp: 1389504412525473176}}
	C := VClock{"": Entry{counter: 10, timestamp: 1389504412525473176}}
	D := VClock{"lion": Entry{counter: 10, timestamp: 1389504412525473176}}

	mq := make(chan bool, 10)

	go func() {
		rec(A, mq)
		rec(B, mq)
		rec(C, mq)
		rec(D, mq)
	}()

	success := false
	failpoint := ""

	var v bool
	if v = <-mq; v == true {
		if v = <-mq; v == true {
			if v = <-mq; v == true {
				if v = <-mq; v == false {
					success = true
				} else {
					failpoint = "a correct VClock"
				}
			} else {
				failpoint = "empty client id"
			}
		} else {
			failpoint = "negative counter"
		}
	} else {
		failpoint = "negative timestamp"
	}

	if !success {
		t.Error("Validation didn't work on", failpoint)
	}
}

func TestIncrement(t *testing.T) {
	n := New("A")
	i := Fresh()
	i.Increment("A")
	if !Equal(n, i) {
		t.Error("Increment not functioning")
	}
}
