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

// WStoreInt16 contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreInt16 struct {
	*fileStore
}

// NewInt16 a new Store object.
func NewInt16(dir, fName string) *WStoreInt16 {
	return &WStoreInt16{
		fileStore: newFileStore(dir, fName),
	}
}

func (s *WStoreInt16) parseInt16(raw string) (int16, bool) {
	v, err := strconv.ParseInt(raw, 10, 16)
	if err != nil {
		errMsg := "parseInt16: invalid "
		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += "range: "
			v = 0
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += "syntax: "
			v = 0
		}
		s.logErr(errors.New(errMsg + strconv.Quote(raw)))
		return int16(v), false
	}
	return int16(v), true
}

// Update creates or updates a new key value.
func (s *WStoreInt16) Update(key string, value int16) error {
	return s.fileStore.update(
		key, strconv.FormatInt(int64(value), 10), float64(value),
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreInt16) Get(
	key string,
) (lastTime time.Time, value int16, ok bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok = s.parseInt16(v)
		if ok {
			return ts, value, true
		}
	}
	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreInt16) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []int16) {
	var t []time.Time
	var v []int16
	s.fileStore.getHistoryDays(
		key, days, func(a Action, ts time.Time, raw string,
		) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				v32, ok := s.parseInt16(raw)
				if ok {
					t = append(t, ts)
					v = append(v, v32)
				}
			}
		})
	return t, v
}
