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
	"strings"
	"time"
)

// WStoreString contains and links the underlying Storage implementation
// and its associated numeric window.
type WStoreString struct {
	*fileStore
	invalidChars   []rune
	numValidValues int
	validValues    []string
}

// NewString a new Store object.
func NewString(dir, fName string) *WStoreString {
	s := newFileStore(dir, fName)
	newWStoreString := new(WStoreString)
	newWStoreString.fileStore = s
	return newWStoreString
}

// SetInvalidChars set the character set of invalid characters.
func (s *WStoreString) SetInvalidChars(c []rune) {
	s.invalidChars = c
}

// SetValidValues sets a list of valid values.
func (s *WStoreString) SetValidValues(v []string) {
	s.validValues = v
	s.numValidValues = len(v)
}

func (s *WStoreString) parseString(raw string) (string, bool) {
	for _, c := range s.invalidChars {
		if strings.ContainsRune(raw, c) {
			s.logErr(errors.New("parseString: invalid character: " +
				strconv.Quote(string(c))))
			return "", false
		}
	}
	if s.numValidValues > 0 {
		found := false
		for i, mi := 0, s.numValidValues; i < mi && !found; i++ {
			found = s.validValues[i] == raw
		}
		if !found {
			s.logErr(errors.New("parseString: invalid value: " +
				strconv.Quote(raw)))
			return "", false
		}
	}
	return raw, true
}

// Update creates or updates a new key value.
func (s *WStoreString) Update(key, value string) error {
	v, ok := s.parseString(value)
	if !ok {
		return ErrInvalidStoreString
	}
	return s.fileStore.update(key, v, float64(len(v)))
}

// Get returns the most recent value for the associated key.
func (s *WStoreString) Get(
	key string,
) (lastTime time.Time, value string, ok bool) {
	ts, v, ok := s.fileStore.get(key)
	if ok {
		value, ok = s.parseString(v)
		if ok {
			return ts, value, true
		}
	}
	return time.Time{}, "", false
}

// GetHistoryDays returns all values made over the specified number of days.
// A zero represent only the current day.
func (s *WStoreString) GetHistoryDays(
	key string, days uint,
) ([]time.Time, []string) {
	var t []time.Time
	var v []string
	s.fileStore.getHistoryDays(key, days,
		func(a Action, ts time.Time, raw string) {
			if a == ActionDelete {
				t = nil
				v = nil
			} else {
				vParsed, ok := s.parseString(raw)
				if ok {
					t = append(t, ts)
					v = append(v, vParsed)
				}
			}
		},
	)
	return t, v
}
