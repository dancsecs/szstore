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

// WStoreInt32 contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreInt32 struct {
	*fileStore
}

// NewInt32 a new Store object.
func NewInt32(dir, fName string) *WStoreInt32 {
	return &WStoreInt32{
		fileStore: newFileStore(dir, fName),
	}
}

func (s *WStoreInt32) parseInt32(raw string) (int32, bool) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		errMsg := "parseInt32: invalid "
		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += "range: "
			v = 0
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += "syntax: "
			v = 0
		}
		s.logMsg(errMsg + strconv.Quote(raw))
		return int32(v), false
	}
	return int32(v), true
}

// Update creates or updates a new key value.
func (s *WStoreInt32) Update(key string, value int32) error {
	return s.fileStore.update(
		key, strconv.FormatInt(int64(value), 10), float64(value),
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreInt32) Get(
	key string,
) (lastTime time.Time, value int32, ok bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok = s.parseInt32(v)
		if ok {
			return ts, value, true
		}
	}
	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreInt32) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []int32) {
	var t []time.Time
	var v []int32
	s.fileStore.getHistoryDays(
		key, days, func(a Action, ts time.Time, raw string,
		) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				v32, ok := s.parseInt32(raw)
				if ok {
					t = append(t, ts)
					v = append(v, v32)
				}
			}
		})
	return t, v
}
