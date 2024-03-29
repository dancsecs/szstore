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
func NewFloat64(dirName, filenameRoot string) *WStoreFloat64 {
	return &WStoreFloat64{
		fileStore: newFileStore(dirName, filenameRoot),
	}
}

func (s *WStoreFloat64) parseFloat64(raw string) (float64, bool) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		errMsg := "parseFloat64: invalid "

		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += rangeErrPrefix
			value = math.NaN()
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += syntaxErrPrefix
			value = 0
		}

		s.logMsg(errMsg + strconv.Quote(raw))

		return value, false
	}

	return value, true
}

// Update creates or updates a new key value.
func (s *WStoreFloat64) Update(key string, value float64) error {
	return s.fileStore.update(
		key, strconv.FormatFloat(value, 'f', -1, 64), value,
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreFloat64) Get(key string) (time.Time, float64, bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok := s.parseFloat64(v)
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
	var (
		timestamps []time.Time
		values     []float64
	)

	s.fileStore.getHistoryDays(
		key, days, func(a Action, timestamp time.Time, raw string,
		) {
			if a == ActionDelete {
				timestamps = nil
				values = nil
			} else {
				v64, ok := s.parseFloat64(raw)
				if ok {
					timestamps = append(timestamps, timestamp)
					values = append(values, v64)
				}
			}
		},
	)

	return timestamps, values
}
