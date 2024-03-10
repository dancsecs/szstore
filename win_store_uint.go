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
	"errors"
	"strconv"
	"time"
)

// WStoreUint contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreUint struct {
	*fileStore
}

// NewUint a new Store object.
func NewUint(dir, fName string) *WStoreUint {
	return &WStoreUint{
		fileStore: newFileStore(dir, fName),
	}
}

func (s *WStoreUint) parseUint(raw string) (uint, bool) {
	v, err := strconv.ParseUint(raw, 10, 0)
	if err != nil {
		errMsg := "parseUint: invalid "
		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += "range: "
			v = 0
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += "syntax: "
			v = 0
		}
		s.logMsg(errMsg + strconv.Quote(raw))
		return uint(v), false
	}
	return uint(v), true
}

// Update creates or updates a new key value.
func (s *WStoreUint) Update(key string, value uint) error {
	return s.fileStore.update(
		key, strconv.FormatUint(uint64(value), 10), float64(value),
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreUint) Get(
	key string,
) (lastTime time.Time, value uint, ok bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok = s.parseUint(v)
		if ok {
			return ts, value, true
		}
	}
	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreUint) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []uint) {
	var t []time.Time
	var v []uint
	s.fileStore.getHistoryDays(
		key, days, func(a Action, ts time.Time, raw string,
		) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				v32, ok := s.parseUint(raw)
				if ok {
					t = append(t, ts)
					v = append(v, v32)
				}
			}
		})
	return t, v
}
