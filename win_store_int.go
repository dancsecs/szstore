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

// WStoreInt contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreInt struct {
	*fileStore
}

// NewInt a new Store object.
func NewInt(dir, fName string) *WStoreInt {
	return &WStoreInt{
		fileStore: newFileStore(dir, fName),
	}
}

func (s *WStoreInt) parseInt(raw string) (int, bool) {
	v, err := strconv.ParseInt(raw, 10, 0)
	if err != nil {
		errMsg := "parseInt: invalid "
		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += "range: "
			v = 0
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += "syntax: "
			v = 0
		}
		s.logMsg(errMsg + strconv.Quote(raw))
		return int(v), false
	}
	return int(v), true
}

// Update creates or updates a new key value.
func (s *WStoreInt) Update(key string, value int) error {
	return s.fileStore.update(
		key, strconv.FormatInt(int64(value), 10), float64(value),
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreInt) Get(key string) (time.Time, int, bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok := s.parseInt(v)
		if ok {
			return ts, value, true
		}
	}
	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreInt) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []int) {
	var t []time.Time
	var v []int
	s.fileStore.getHistoryDays(
		key, days, func(a Action, ts time.Time, raw string,
		) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				v32, ok := s.parseInt(raw)
				if ok {
					t = append(t, ts)
					v = append(v, v32)
				}
			}
		})
	return t, v
}
