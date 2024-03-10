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
	"time"

	"github.com/dancsecs/sztest"
)

func setupWStoreUint32WithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreUint32) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	uint32Store := NewUint32(dirName, filename)
	uint32Store.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, uint32Store
}

func validateUint32History(
	chk *sztest.Chk,
	uint32Store *WStoreUint32,
	datKey string,
	days uint, //nolint:unparam // Always a 0.
	expTSlice []string,
	expVSlice []uint32,
) {
	chk.T().Helper()

	timestamp, value, ok := uint32Store.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Uint32(value, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := uint32Store.GetHistoryDays(datKey, days)

	tSlice := make([]string, len(tsSlice))
	for i, ts := range tsSlice {
		tSlice[i] = ts.Format(fmtTimeStamp)
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.Uint32Slice(vSlice, expVSlice, 0)
}

func Test_WStoreUint32_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, uint32Store := setupWStoreUint32WithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|4294967296"},
		}),
	)

	chk.NoErr(uint32Store.Open())
	defer closeAndLogIfError(uint32Store)

	validateUint32History(chk, uint32Store, "key1", 0, // advances to clkNano2
		[]string{},
		[]uint32{},
	)

	validateUint32History(chk, uint32Store, "key2", 0, // advances to clkNano2
		[]string{},
		[]uint32{},
	)

	chk.NoErr(uint32Store.Update("key1", 200)) // clkNano4
	chk.NoErr(uint32Store.Update("key2", 400)) // clkNano5

	validateUint32History(chk, uint32Store, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]uint32{200},
	)

	validateUint32History(chk, uint32Store, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]uint32{400},
	)

	chk.NoErr(uint32Store.Delete("key1")) // clkNano8
	chk.NoErr(uint32Store.Delete("key2")) // clkNano9

	validateUint32History(chk, uint32Store, "key1", 0, // advances to clkNano10
		[]string{},
		[]uint32{},
	)

	validateUint32History(chk, uint32Store, "key2", 0, // advances to clkNano11
		[]string{},
		[]uint32{},
	)

	chk.NoErr(uint32Store.Update("key1", 222)) // clkNano12
	chk.NoErr(uint32Store.Update("key2", 444)) // clkNano13

	validateUint32History(chk, uint32Store, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]uint32{222},
	)

	validateUint32History(chk, uint32Store, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]uint32{444},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseUint32: invalid syntax: "abc"`,
		`parseUint32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint32: invalid range: "4294967296"`,
		`parseUint32: invalid range: "4294967296"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|4294967296"`,
		`parseUint32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint32: invalid range: "4294967296"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|4294967296"`,
		`get("key1"): unknown data key`,
		`parseUint32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseUint32: invalid range: "4294967296"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|4294967296"`,
		`parseUint32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint32: invalid range: "4294967296"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|4294967296"`,
	)
}
