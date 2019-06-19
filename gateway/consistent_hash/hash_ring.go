/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package consistenthash

// HashRing Sorted hash key array
type HashRing []int

// Len: Its length
func (hr HashRing) Len() int {
	return len(hr)
}

// Less: compare method
func (hr HashRing) Less(i, j int) bool {
	return hr[i] < hr[j]
}

// Swap: swap method
func (hr HashRing) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}
