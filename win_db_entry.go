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
	"strconv"
	"time"
)

// WindowEntry represents a linked list of measurements belonging to both
// an individual window and the collection of all windows associated with
// a specific data key.
type windowEntry struct {
	timestamp time.Time
	value     float64
	prev      *windowEntry
	next      *windowEntry
}

func (e *windowEntry) newHead(
	entry *windowEntry, timestamp time.Time, value float64,
) *windowEntry {
	if entry == nil {
		entry = new(windowEntry)
	}

	entry.timestamp = timestamp
	entry.value = value
	entry.next = e

	if e != nil {
		e.prev = entry
	}

	return entry
}

func (e *windowEntry) String() string {
	if e == nil {
		return "<nil>"
	}

	return e.timestamp.Format(fmtTimeStamp) +
		" - " +
		strconv.FormatFloat(e.value, 'g', -1, 64)
}

// Window represents a specific collection of measurements included in a
// window's time period.
type window struct {
	datKey     string
	winKey     string
	period     time.Duration
	newest     *windowEntry
	oldest     *windowEntry
	count      uint64
	total      float64
	avg        float64
	thresholds []*threshold
}

func newWindow(datKey, winKey string, timePeriod time.Duration) *window {
	if timePeriod < time.Nanosecond {
		timePeriod = time.Nanosecond
	}

	newWin := new(window)
	newWin.datKey = datKey
	newWin.winKey = winKey
	newWin.period = timePeriod

	return newWin
}

func (w *window) addThreshold(
	lowCritical, lowWarning, highWarning, highCritical float64,
	callback ThresholdNotifyFunc,
) error {
	threshold, err := newThreshold(
		w.datKey, w.winKey,
		lowCritical, lowWarning, highWarning, highCritical,
		callback,
	)
	if err == nil {
		w.thresholds = append(w.thresholds, threshold)
	}

	return err
}

func (w *window) add(newEntry *windowEntry) {
	w.newest = newEntry
	if w.oldest == nil {
		// First entry.
		w.oldest = newEntry
	}

	w.count++
	w.total += newEntry.value
	w.trim()
	w.avg = w.total / float64(w.count)

	for _, t := range w.thresholds {
		t.check(w.avg)
	}
}

func (w *window) trim() {
	for {
		if w.newest == nil {
			return
		}

		if w.newest == w.oldest { // Keep last measurement.
			return
		}

		if w.newest.timestamp.Sub(w.oldest.timestamp) <= w.period {
			return
		}

		w.count--
		w.total -= w.oldest.value
		w.oldest = w.oldest.prev
	}
}

func (w *window) delete() {
	w.oldest = nil
	w.newest = nil
	w.count = 0
	w.total = 0
	w.avg = 0
}

func (w *window) getAvg() (float64, error) {
	if w.count < 1 {
		return 0, ErrNoWinData
	}

	return w.avg, nil
}

func (w *window) getCount() (uint64, error) {
	if w.count < 1 {
		return 0, ErrNoWinData
	}

	return w.count, nil
}

// String returns a string representation of a window structure.
func (w *window) String() string {
	average, _ := w.getAvg()
	count, _ := w.getCount()

	return fmt.Sprintf(
		"datKey: %s winKey: %s Period: "+
			"%v Newest: %v Oldest: %v Count: %d Avg: %g",
		w.datKey, w.winKey, w.period, w.newest, w.oldest, count, average,
	)
}
