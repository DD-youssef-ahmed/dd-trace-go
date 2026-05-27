// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2026 Datadog, Inc.

package tracer

import "sync"

var spanPool = sync.Pool{
	New: func() any { return &Span{} },
}

func acquireSpan(poolEnabled bool) *Span {
	if poolEnabled {
		s := spanPool.Get().(*Span)
		s.clear()
		return s
	}
	return &Span{
		metrics: make(map[string]float64, 1),
	}
}

func releaseSpan(s *Span, poolEnabled bool) {
	if !poolEnabled {
		return
	}
	spanPool.Put(s)
}

func releaseSpans(spans []*Span, poolEnabled bool) {
	if !poolEnabled {
		return
	}
	for _, s := range spans {
		spanPool.Put(s)
	}
}
