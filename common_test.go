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
	daysAgo int, dirName, filenameRoot string, data [][2]string,
) error {
	chk.T().Helper()

	fileData := ""
	resetClk := chk.ClockOffsetDay(-daysAgo)

	chk.ClockAddSub(sztest.ClockSubNano)

	if daysAgo != 0 {
		defer resetClk()
	}

	for _, entry := range data {
		var timestamp string

		if entry[0] == "" {
			timestamp = chk.ClockNextFmtNano()
		} else {
			timestamp = entry[0]
		}

		fileData += timestamp + entry[1] + "\n"
	}

	fPath := filepath.Join(
		dirName, filenameRoot+"_"+chk.ClockLastFmtDate()+fileExtension,
	)
	err := os.WriteFile(fPath, []byte(fileData), 0o0600)

	chk.AddSub("{{hPath"+strconv.FormatInt(int64(daysAgo), base10)+"}}", fPath)

	return err //nolint:wrapcheck // Ok.
}
