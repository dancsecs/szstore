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

// WStoreFloat32 contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreFloat32 struct {
	*fileStore
}

// NewFloat32 a new Store object.
func NewFloat32(dirName, filenameRoot string) *WStoreFloat32 {
	return &WStoreFloat32{
		fileStore: newFileStore(dirName, filenameRoot),
	}
}

func (s *WStoreFloat32) parseFloat32(raw string) (float32, bool) {
	value, err := strconv.ParseFloat(raw, 32)
	if err != nil {
		errMsg := "parseFloat32: invalid "

		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += rangeErrPrefix
			value = math.NaN()
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += syntaxErrPrefix
			value = 0
		}

		s.logMsg(errMsg + strconv.Quote(raw))

		return float32(value), false
	}

	return float32(value), true
}

// Update creates or updates a new key value.
func (s *WStoreFloat32) Update(key string, value float32) error {
	return s.fileStore.update(
		key, strconv.FormatFloat(float64(value), 'f', -1, 64), float64(value),
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreFloat32) Get(key string) (time.Time, float32, bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok := s.parseFloat32(v)
		if ok {
			return ts, value, true
		}
	}

	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreFloat32) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []float32) {
	var (
		timestamps []time.Time
		values     []float32
	)

	s.fileStore.getHistoryDays(
		key, days, func(a Action, timestamp time.Time, raw string,
		) {
			if a == ActionDelete {
				timestamps = nil
				values = nil
			} else {
				v32, ok := s.parseFloat32(raw)
				if ok {
					timestamps = append(timestamps, timestamp)
					values = append(values, v32)
				}
			}
		},
	)

	return timestamps, values
}
