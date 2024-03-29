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

// WStoreUint16 contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreUint16 struct {
	*fileStore
}

// NewUint16 a new Store object.
func NewUint16(dirName, filenameRoot string) *WStoreUint16 {
	return &WStoreUint16{
		fileStore: newFileStore(dirName, filenameRoot),
	}
}

func (s *WStoreUint16) parseUint16(raw string) (uint16, bool) {
	value, err := strconv.ParseUint(raw, 10, 16)
	if err != nil {
		errMsg := "parseUint16: invalid "

		switch {
		case errors.Is(err, strconv.ErrRange):
			errMsg += rangeErrPrefix
			value = 0
		default: //  errors.Is(err, strconv.ErrSyntax):
			errMsg += syntaxErrPrefix
			value = 0
		}

		s.logMsg(errMsg + strconv.Quote(raw))

		return uint16(value), false
	}

	return uint16(value), true
}

// Update creates or updates a new key value.
func (s *WStoreUint16) Update(key string, value uint16) error {
	return s.fileStore.update(
		key, strconv.FormatUint(uint64(value), 10), float64(value),
	)
}

// Get returns the most recent value for the associated key.
func (s *WStoreUint16) Get(key string) (time.Time, uint16, bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok := s.parseUint16(v)
		if ok {
			return ts, value, true
		}
	}

	return time.Time{}, 0.0, false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreUint16) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []uint16) {
	var (
		timestamps []time.Time
		values     []uint16
	)

	s.fileStore.getHistoryDays(
		key, days, func(a Action, timestamp time.Time, raw string,
		) {
			if a == ActionDelete {
				timestamps = nil
				values = nil
			} else {
				v32, ok := s.parseUint16(raw)
				if ok {
					timestamps = append(timestamps, timestamp)
					values = append(values, v32)
				}
			}
		},
	)

	return timestamps, values
}
