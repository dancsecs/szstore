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
	"math"
	"strconv"
	"time"
)

// WStoreFloat64 contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreFloat64 struct {
	*fileStore
}

// NewFloat64 a new Store object.
func NewFloat64(dir, fName string) *WStoreFloat64 {
	return &WStoreFloat64{
		fileStore: newFileStore(dir, fName),
	}
}

func (s *WStoreFloat64) parseFloat64(raw string) (float64, bool) {
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		errMsg := "parseFloat64: invalid "
		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += "range: "
			v = math.NaN()
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += "syntax: "
			v = 0
		}
		s.logMsg(errMsg + strconv.Quote(raw))
		return v, false
	}
	return v, true
}

// Update creates or updates a new key value.
func (s *WStoreFloat64) Update(key string, value float64) error {
	return s.fileStore.update(
		key, strconv.FormatFloat(value, 'f', -1, 64), value,
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreFloat64) Get(
	key string,
) (lastTime time.Time, value float64, ok bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok = s.parseFloat64(v)
		if ok {
			return ts, value, true
		}
	}
	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreFloat64) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []float64) {
	var t []time.Time
	var v []float64

	s.fileStore.getHistoryDays(
		key, days, func(a Action, ts time.Time, raw string,
		) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				v64, ok := s.parseFloat64(raw)
				if ok {
					t = append(t, ts)
					v = append(v, v64)
				}
			}
		})
	return t, v
}
