package vclock

import "testing"

func TestEqual(t *testing.T) {
	// Different Timestamps, same values
	A := VClock{"lion": Entry{Counter: 1309, Timestamp: 1389503545254049010}, "gazelle": Entry{Counter: 1, Timestamp: 1389503545254049981}}
	B := VClock{"lion": Entry{Counter: 1309, Timestamp: 1389503545254050111}, "gazelle": Entry{Counter: 1, Timestamp: 1389503545254050391}}

	if !Equal(A, B) {
		defer t.Error("Different Timestamps should be equal")
	}

	C, D := Fresh(), Fresh()

	if !Equal(C, D) {
		defer t.Error("Empty VClocks should be equal")
	}
}

func TestValidationShort(t *testing.T) {
	A := VClock{"lion": Entry{Counter: 10, Timestamp: -5}}
	B := VClock{"lion": Entry{Counter: -10, Timestamp: 1389504412525473176}}
	C := VClock{"": Entry{Counter: 10, Timestamp: 1389504412525473176}}
	D := VClock{"lion": Entry{Counter: 10, Timestamp: 1389504412525473176}}
	a, b, c, d := A.IsValid(), B.IsValid(), C.IsValid(), D.IsValid()
	switch {
	case a:
		t.Error("validated negative Timestamp")
	case b:
		t.Error("validated negative Counter")
	case c:
		t.Error("validated empty client id")
	case !d:
		t.Error("invalidated correct VClock")
	}
}

func TestMerge(t *testing.T) {
	A := VClock{"lion": Entry{Counter: 1309, Timestamp: 1}, "gazelle": Entry{Counter: 1, Timestamp: 13}, "zebra": Entry{Counter: 7, Timestamp: 9}}
	B := VClock{"lion": Entry{Counter: 1, Timestamp: 138}, "gazelle": Entry{Counter: 6, Timestamp: 1389}}

	C := Merge([]VClock{A, B})
	D := VClock{"lion":Entry{Counter:1309, Timestamp:138}, "gazelle":Entry{Counter:6, Timestamp:1389}, "zebra":Entry{Counter:7, Timestamp:9}}

	// we want Timestamps to merge too.
	if !Equal(C, D) {
		t.Error("merge failed:\n", C, "!=", D)
	}
}

func TestCompare(t *testing.T) {
	// stub
}

func TestLatest(t *testing.T) {
	A := New("lion")
	B := New("lion")
	C := New("lion")
	B.Increment("gazelle")
	C.Increment("zebra")

	clocks := make(map[string]VClock, 3)
	clocks["A"] = A
	clocks["B"] = B
	clocks["C"] = C

	if 2 != len(Latest(clocks)) {
		t.Error("didn't catch the branch")
	}
}

// Benchmarks

func BenchmarkMerge(b *testing.B) {
	A := VClock{"lion": Entry{Counter: 1309, Timestamp: 1}, "gazelle": Entry{Counter: 1, Timestamp: 13}, "zebra": Entry{Counter: 7, Timestamp: 9}}
	B := VClock{"lion": Entry{Counter: 1, Timestamp: 138}, "gazelle": Entry{Counter: 6, Timestamp: 1389}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Merge([]VClock{A, B})
	}
}

func BenchmarkCompare(b *testing.B) {
	A := VClock{"lion": Entry{Counter: 1309, Timestamp: 1}, "gazelle": Entry{Counter: 1, Timestamp: 13}, "zebra": Entry{Counter: 7, Timestamp: 9}}
	B := VClock{"lion": Entry{Counter: 1, Timestamp: 138}, "gazelle": Entry{Counter: 6, Timestamp: 1389}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Compare(A, B)
	}
}

func BenchmarkEqual(b *testing.B) {
	A := VClock{"lion": Entry{Counter: 1309, Timestamp: 1}, "gazelle": Entry{Counter: 1, Timestamp: 13}, "zebra": Entry{Counter: 7, Timestamp: 9}}
	B := VClock{"lion": Entry{Counter: 1, Timestamp: 138}, "gazelle": Entry{Counter: 6, Timestamp: 1389}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Equal(A, B)
	}
}

func BenchmarkIncrement(b *testing.B) {
	A := VClock{"lion": Entry{Counter: 1309, Timestamp: 1}, "gazelle": Entry{Counter: 1, Timestamp: 13}, "zebra": Entry{Counter: 7, Timestamp: 9}}

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
	A := VClock{"lion": Entry{Counter: 10, Timestamp: -5}}
	B := VClock{"lion": Entry{Counter: -10, Timestamp: 1389504412525473176}}
	C := VClock{"": Entry{Counter: 10, Timestamp: 1389504412525473176}}
	D := VClock{"lion": Entry{Counter: 10, Timestamp: 1389504412525473176}}

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
			failpoint = "negative Counter"
		}
	} else {
		failpoint = "negative Timestamp"
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
