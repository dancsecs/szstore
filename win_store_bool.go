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
	"strconv"
	"time"
)

// WStoreBool contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreBool struct {
	*fileStore
	//	rwMutex sync.RWMutex
}

// NewBool a new Store object.
func NewBool(dir, fName string) *WStoreBool {
	store := newFileStore(dir, fName)

	return &WStoreBool{
		fileStore: store,
	}
}

func (s *WStoreBool) parseBool(raw string) (bool, bool) {
	switch raw {
	case "false":
		return false, true
	case "true":
		return true, true
	default:
		s.logMsg(
			"parseBool: invalid syntax: " + strconv.Quote(raw),
		)

		return false, false
	}
}

// Update creates or updates a new key value.
func (s *WStoreBool) Update(key string, value bool) error {
	var v float64
	if value {
		v = 1.0
	}

	return s.fileStore.update(key, strconv.FormatBool(value), v)
}

// Get returns the most recent value for the associated key.
func (s *WStoreBool) Get(datKey string) (time.Time, bool, bool) {
	ts, v, ok := s.fileStore.get(datKey)
	if ok {
		value, ok := s.parseBool(v)
		if ok {
			return ts, value, true
		}
	}

	return time.Time{}, false, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreBool) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []bool) {
	var (
		t []time.Time
		v []bool
	)

	s.fileStore.getHistoryDays(
		key, days, func(a Action, ts time.Time, raw string,
		) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				vParsed, ok := s.parseBool(raw)
				if ok {
					t = append(t, ts)
					v = append(v, vParsed)
				}
			}
		},
	)

	return t, v
}

// AddWindowThreshold adds the provided threshold data to the indicated numeric
// window.
func (s *WStoreBool) AddWindowThreshold(datKey, winKey string,
	lowCritical, lowWarning, highWarning, highCritical float64,
	f ThresholdCallbackFunc,
) error {
	if lowCritical < 0.0 || lowCritical > 1.0 ||
		lowWarning < 0.0 || lowWarning > 1.0 ||
		highWarning < 0.0 || highWarning > 1.0 ||
		highCritical < 0.0 || highCritical > 1.0 ||
		false {
		return ErrInvalidBoolThreshold
	}

	return s.fileStore.AddWindowThreshold(datKey, winKey,
		lowCritical, lowWarning, highWarning, highCritical,
		f,
	)
}
