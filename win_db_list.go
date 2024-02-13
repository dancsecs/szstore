/*
   Szerszam Windowed Storage Library: szstore.
   Copyright (C) 2023, 2024  Leslie Dancsecs

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package szstore

import (
	"fmt"
	"sort"
	"time"
)

// winDB contains all information necessary to keep l list of entries
// representing all time periods specified buy the list of windows.
type winDB struct {
	datKey      string
	newestEntry *windowEntry
	oldestEntry *windowEntry
	maxPeriod   time.Duration
	windows     map[string]*window
	winKeys     []string
	cachedEntry *windowEntry
}

// newWinDB creates a new DB objects to contain all windowed entries.
func newWinDB(datKey string) *winDB {
	return &winDB{
		datKey:    datKey,
		maxPeriod: time.Nanosecond,
		windows:   make(map[string]*window),
	}
}

// addWindow includes a new time period to account all entries for.
func (wdb *winDB) addWindow(
	winKey string, p time.Duration,
) error {
	if _, ok := wdb.windows[winKey]; ok {
		return ErrDupWinKey
	}

	newWin := newWindow(wdb.datKey, winKey, p)

	wdb.windows[winKey] = newWin
	if p > wdb.maxPeriod {
		wdb.maxPeriod = p
	}
	wdb.winKeys = append(wdb.winKeys, winKey)
	sort.Strings(wdb.winKeys)

	return nil
}

// addValue incorporates a new value into into the underlying store.
func (wdb *winDB) addValue(t time.Time, f float64) {
	e := wdb.cachedEntry
	if e != nil {
		wdb.cachedEntry = e.next
	}
	wdb.newestEntry = wdb.newestEntry.newHead(e, t, f)
	if wdb.oldestEntry == nil {
		wdb.oldestEntry = wdb.newestEntry
	}
	for _, wk := range wdb.winKeys {
		wdb.windows[wk].add(wdb.newestEntry)
	}
	wdb.trim()
}

// getAvg returns the average over the entire sample.
func (wdb *winDB) getAvg(winKey string) (float64, error) {
	dw, ok := wdb.windows[winKey]
	if !ok {
		return 0, ErrUnknownWinKey
	}
	return dw.getAvg()
}

// getCount returns the number of samples in the window.
func (wdb *winDB) getCount(winKey string) (uint64, error) {
	dw, ok := wdb.windows[winKey]
	if !ok {
		return 0, ErrUnknownWinKey
	}
	return dw.getCount()
}

// delete resets the named window.
func (wdb *winDB) delete() {
	for _, w := range wdb.windows {
		w.delete()
	}
	if wdb.oldestEntry != nil {
		wdb.oldestEntry.next = wdb.cachedEntry
	}
	wdb.cachedEntry = wdb.newestEntry
	wdb.newestEntry = nil
	wdb.oldestEntry = nil
}

func (wdb *winDB) trim() {
	nt := wdb.newestEntry.t
	for wdb.oldestEntry.prev != nil &&
		nt.Sub(wdb.oldestEntry.t) > wdb.maxPeriod {
		//	log.Printf("Removing oldest")
		e := wdb.oldestEntry
		wdb.oldestEntry = e.prev
		wdb.oldestEntry.next = nil // Just to help garbage collector.
		e.prev = nil
		e.next = wdb.cachedEntry
		wdb.cachedEntry = e
	}
}

func (wdb *winDB) addThreshold(winKey string,
	lowCritical, lowWarning, highWarning, highCritical float64,
	f ThresholdCallbackFunc,
) error {
	dw, ok := wdb.windows[winKey]
	if !ok {
		return ErrUnknownWinKey
	}
	return dw.addThreshold(
		lowCritical, lowWarning, highWarning, highCritical, f,
	)
}

func (wdb *winDB) count() int {
	n := 0
	e := wdb.newestEntry

	for e != nil {
		n++
		e = e.next
	}
	return n
}

func (wdb *winDB) cachedCount() int {
	n := 0
	e := wdb.cachedEntry

	for e != nil {
		n++
		e = e.next
	}
	return n
}

func (wdb *winDB) String() string {
	r := "" +
		"winDB: datKey: " + wdb.datKey +
		" maxPeriod: " + fmt.Sprintf("%v", wdb.maxPeriod) +
		fmt.Sprintf(
			"\n\tFirst: %v Last: %v Active: %d Cached: %d",
			wdb.newestEntry, wdb.oldestEntry, wdb.count(), wdb.cachedCount(),
		)

	for _, w := range wdb.winKeys {
		r += "\n\t\t" + wdb.windows[w].String()
	}
	return r
}
