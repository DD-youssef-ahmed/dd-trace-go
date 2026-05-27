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
		return spanPool.Get().(*Span)
	}
	return &Span{
		metrics: make(map[string]float64, 1),
	}
}

func releaseSpans(spans []*Span) {
	for _, s := range spans {
		s.clear()
		spanPool.Put(s)
	}
}
