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
	"os"
	"path/filepath"
	"strconv"

	"github.com/dancsecs/sztest"
)

func buildHistoryFile(
	chk *sztest.Chk,
	daysAgo int, d, f string, data [][2]string,
) error {
	const base10 = 10
	chk.T().Helper()
	fd := ""
	resetClk := chk.ClockOffsetDay(-daysAgo)
	chk.ClockAddSub(sztest.ClockSubNano)
	if daysAgo != 0 {
		defer resetClk()
	}

	for _, e := range data {
		ts := ""
		if e[0] == "" {
			ts = chk.ClockNextFmtNano()
		} else {
			ts = e[0]
		}
		fd += ts + e[1] + "\n"
	}
	fPath := filepath.Join(d, f+"_"+chk.ClockLastFmtDate()+fileExtension)
	err := os.WriteFile(fPath, []byte(fd), 0644)
	chk.AddSub("{{hPath"+strconv.FormatInt(int64(daysAgo), base10)+"}}", fPath)
	return err
}
