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

// Action indicates the type of storage transaction.
type Action byte

// Action constants.
const (
	ActionUpdate = 'U'
	ActionDelete = 'D'
)

func (a Action) String() string {
	switch a {
	case ActionUpdate:
		return "U - Update"
	case ActionDelete:
		return "D - Delete"
	default:
		return "? - ACTION(" + string(a) + ")"
	}
}

// IsOK checks that the action is valid.
func (a Action) IsOK() bool {
	return a == ActionUpdate || a == ActionDelete
}
