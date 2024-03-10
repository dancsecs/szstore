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

func setupWStoreUint64WithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreUint64) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	uint64Store := NewUint64(dirName, filename)
	uint64Store.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, uint64Store
}

func validateUint64History(
	chk *sztest.Chk,
	uint64Store *WStoreUint64,
	datKey string,
	days uint,
	expTSlice []string,
	expVSlice []uint64,
) {
	chk.T().Helper()

	timestamp, value, ok := uint64Store.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Uint64(value, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := uint64Store.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.Uint64Slice(vSlice, expVSlice, 0)
}

func Test_WStoreUint64_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, uint64Store := setupWStoreUint64WithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|18446744073709551616"},
		}),
	)

	chk.NoErr(uint64Store.Open())
	defer closeAndLogIfError(uint64Store)

	validateUint64History(chk, uint64Store, "key1", 0, // advances to clkNano2
		[]string{},
		[]uint64{},
	)

	validateUint64History(chk, uint64Store, "key2", 0, // advances to clkNano2
		[]string{},
		[]uint64{},
	)

	chk.NoErr(uint64Store.Update("key1", 200)) // clkNano4
	chk.NoErr(uint64Store.Update("key2", 400)) // clkNano5

	validateUint64History(chk, uint64Store, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]uint64{200},
	)

	validateUint64History(chk, uint64Store, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]uint64{400},
	)

	chk.NoErr(uint64Store.Delete("key1")) // clkNano8
	chk.NoErr(uint64Store.Delete("key2")) // clkNano9

	validateUint64History(chk, uint64Store, "key1", 0, // advances to clkNano10
		[]string{},
		[]uint64{},
	)

	validateUint64History(chk, uint64Store, "key2", 0, // advances to clkNano11
		[]string{},
		[]uint64{},
	)

	chk.NoErr(uint64Store.Update("key1", 222)) // clkNano12
	chk.NoErr(uint64Store.Update("key2", 444)) // clkNano13

	validateUint64History(chk, uint64Store, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]uint64{222},
	)

	validateUint64History(chk, uint64Store, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]uint64{444},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseUint64: invalid syntax: "abc"`,
		`parseUint64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint64: invalid range: "18446744073709551616"`,
		`parseUint64: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
		`parseUint64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint64: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
		`get("key1"): unknown data key`,
		`parseUint64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseUint64: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
		`parseUint64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint64: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
	)
}
