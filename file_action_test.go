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
	"testing"

	"github.com/dancsecs/sztest"
)

func TestImpl_Action(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	updateAction := Action('U')
	deleteAction := Action('D')
	badAction := Action('B')

	chk.True(updateAction.IsOK())
	chk.True(deleteAction.IsOK())
	chk.False(badAction.IsOK())

	chk.Str(updateAction.String(), "U - Update")
	chk.Str(deleteAction.String(), "D - Delete")
	chk.Str(badAction.String(), "? - ACTION(B)")
}
