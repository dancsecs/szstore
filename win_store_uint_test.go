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

func setupWStoreUintWithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreUint) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	uintStore := NewUint(dirName, filename)
	uintStore.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, uintStore
}

func validateUintHistory(
	chk *sztest.Chk,
	uintStore *WStoreUint,
	datKey string,
	days uint, //nolint:unparam // Always a 0.
	expTSlice []string,
	expVSlice []uint,
) {
	chk.T().Helper()

	timestamp, value, ok := uintStore.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Uint(value, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := uintStore.GetHistoryDays(datKey, days)

	tSlice := make([]string, len(tsSlice))
	for i, ts := range tsSlice {
		tSlice[i] = ts.Format(fmtTimeStamp)
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.UintSlice(vSlice, expVSlice, 0)
}

//nolint:funlen // Ok.
func Test_WStoreUint_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, uintStore := setupWStoreUintWithClock(
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

	chk.NoErr(uintStore.Open())
	defer closeAndLogIfError(uintStore)

	validateUintHistory(chk, uintStore, "key1", 0, // next clk:clkNano2
		[]string{},
		[]uint{},
	)

	validateUintHistory(chk, uintStore, "key2", 0, // next clk:clkNano2
		[]string{},
		[]uint{},
	)

	chk.NoErr(uintStore.Update("key1", 200)) // clkNano4
	chk.NoErr(uintStore.Update("key2", 400)) // clkNano5

	validateUintHistory(chk, uintStore, "key1", 0, // next clk:clkNano6
		[]string{"{{clkNano4}}"},
		[]uint{200},
	)

	validateUintHistory(chk, uintStore, "key2", 0, // next clk:clkNano7
		[]string{"{{clkNano5}}"},
		[]uint{400},
	)

	chk.NoErr(uintStore.Delete("key1")) // clkNano8
	chk.NoErr(uintStore.Delete("key2")) // clkNano9

	validateUintHistory(chk, uintStore, "key1", 0, // next clk:clkNano10
		[]string{},
		[]uint{},
	)

	validateUintHistory(chk, uintStore, "key2", 0, // next clk:clkNano11
		[]string{},
		[]uint{},
	)

	chk.NoErr(uintStore.Update("key1", 222)) // clkNano12
	chk.NoErr(uintStore.Update("key2", 444)) // clkNano13

	validateUintHistory(chk, uintStore, "key1", 0, // next clk:clkNano14
		[]string{"{{clkNano12}}"},
		[]uint{222},
	)

	validateUintHistory(chk, uintStore, "key2", 0, // next clk:clkNano15
		[]string{"{{clkNano13}}"},
		[]uint{444},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseUint: invalid syntax: "abc"`,
		`parseUint: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint: invalid range: "18446744073709551616"`,
		`parseUint: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
		`parseUint: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
		`get("key1"): unknown data key`,
		`parseUint: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseUint: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
		`parseUint: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint: invalid range: "18446744073709551616"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|18446744073709551616"`,
	)
}
