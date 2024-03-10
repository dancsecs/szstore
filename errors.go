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
	"io"
	"log"
)

//
// Data key errors.
//

// Public Errors.
var (
	ErrInvalidDatKey         = errors.New("invalid data key")
	ErrUnknownDatKey         = errors.New("unknown data key")
	ErrUnknownWinKey         = errors.New("unknown window key")
	ErrDupWinKey             = errors.New("duplicate window key")
	ErrNoWinData             = errors.New("no window data")
	ErrUnknownAction         = errors.New("unknown callback action")
	ErrNilNotifyFunc         = errors.New("invalid nil notify function")
	ErrInvalidRecord         = errors.New("invalid record")
	ErrOpenedWindow          = errors.New("invalid add window on opened db")
	ErrOpenedWindowThreshold = errors.New(
		"invalid add window threshold on opened db",
	)
	ErrOpenedWindowNotifyFunc = errors.New(
		"invalid add window notify function on opened db",
	)
	ErrInvalidBoolThreshold = errors.New(
		"boolean thresholds must be >=0 and <= 1",
	)
	ErrInvalidThresholdOrder = errors.New(
		"invalid order;" +
			" need lowCritical <= lowWarning <= highWarning <= highCritical",
	)
	ErrInvalidStoreString = errors.New(
		"invalid store string",
	)
)

func closeAndLogIfError(f io.Closer) {
	err := f.Close()
	if err != nil {
		log.Print("close caused: ", err)
	}
}
