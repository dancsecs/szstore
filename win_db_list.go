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
	newWinDB := new(winDB)
	newWinDB.datKey = datKey
	newWinDB.maxPeriod = time.Nanosecond
	newWinDB.windows = make(map[string]*window)

	return newWinDB
}

// addWindow includes a new time period to account all entries for.
func (wdb *winDB) addWindow(
	winKey string, timePeriod time.Duration,
) error {
	if _, ok := wdb.windows[winKey]; ok {
		return ErrDupWinKey
	}

	newWin := newWindow(wdb.datKey, winKey, timePeriod)

	wdb.windows[winKey] = newWin
	if timePeriod > wdb.maxPeriod {
		wdb.maxPeriod = timePeriod
	}

	wdb.winKeys = append(wdb.winKeys, winKey)
	sort.Strings(wdb.winKeys)

	return nil
}

// addValue incorporates a new value into the underlying store.
func (wdb *winDB) addValue(timestamp time.Time, value float64) {
	e := wdb.cachedEntry
	if e != nil {
		wdb.cachedEntry = e.next
	}

	wdb.newestEntry = wdb.newestEntry.newHead(e, timestamp, value)

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
	nt := wdb.newestEntry.timestamp
	for wdb.oldestEntry.prev != nil &&
		nt.Sub(wdb.oldestEntry.timestamp) > wdb.maxPeriod {
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
	notifyFunc ThresholdNotifyFunc,
) error {
	dw, ok := wdb.windows[winKey]
	if !ok {
		return ErrUnknownWinKey
	}

	return dw.addThreshold(
		lowCritical, lowWarning, highWarning, highCritical, notifyFunc,
	)
}

func (wdb *winDB) count() int {
	numEntries := 0
	entry := wdb.newestEntry

	for entry != nil {
		numEntries++
		entry = entry.next
	}

	return numEntries
}

func (wdb *winDB) cachedCount() int {
	numEntries := 0
	entry := wdb.cachedEntry

	for entry != nil {
		numEntries++
		entry = entry.next
	}

	return numEntries
}

func (wdb *winDB) String() string {
	result := "" +
		"winDB: datKey: " + wdb.datKey +
		" maxPeriod: " + fmt.Sprintf("%v", wdb.maxPeriod) +
		fmt.Sprintf(
			"\n\tFirst: %v Last: %v Active: %d Cached: %d",
			wdb.newestEntry, wdb.oldestEntry, wdb.count(), wdb.cachedCount(),
		)

	for _, w := range wdb.winKeys {
		result += "\n\t\t" + wdb.windows[w].String()
	}

	return result
}
