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

// WStoreInt8 contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreInt8 struct {
	*fileStore
}

// NewInt8 a new Store object.
func NewInt8(dir, fName string) *WStoreInt8 {
	return &WStoreInt8{
		fileStore: newFileStore(dir, fName),
	}
}

func (s *WStoreInt8) parseInt8(raw string) (int8, bool) {
	v, err := strconv.ParseInt(raw, 10, 8)
	if err != nil {
		errMsg := "parseInt8: invalid "
		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += "range: "
			v = 0
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += "syntax: "
			v = 0
		}
		s.logMsg(errMsg + strconv.Quote(raw))
		return int8(v), false
	}
	return int8(v), true
}

// Update creates or updates a new key value.
func (s *WStoreInt8) Update(key string, value int8) error {
	return s.fileStore.update(
		key, strconv.FormatInt(int64(value), 10), float64(value),
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreInt8) Get(key string) (time.Time, int8, bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok := s.parseInt8(v)
		if ok {
			return ts, value, true
		}
	}
	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreInt8) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []int8) {
	var t []time.Time
	var v []int8
	s.fileStore.getHistoryDays(
		key, days, func(a Action, ts time.Time, raw string,
		) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				v32, ok := s.parseInt8(raw)
				if ok {
					t = append(t, ts)
					v = append(v, v32)
				}
			}
		})
	return t, v
}
