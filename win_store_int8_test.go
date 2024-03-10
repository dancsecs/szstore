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

func setupWStoreInt8WithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreInt8) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	int8Store := NewInt8(dirName, filename)
	int8Store.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, int8Store
}

func validateInt8History(
	chk *sztest.Chk,
	int8Store *WStoreInt8,
	datKey string,
	days uint,
	expTSlice []string,
	expVSlice []int8,
) {
	chk.T().Helper()

	timestamp, value, ok := int8Store.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Int8(value, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := int8Store.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.Int8Slice(vSlice, expVSlice, 0)
}

func Test_WStoreInt8_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, int8Store := setupWStoreInt8WithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|128"},
		}),
	)

	chk.NoErr(int8Store.Open())
	defer closeAndLogIfError(int8Store)

	validateInt8History(chk, int8Store, "key1", 0, // advances to clkNano2
		[]string{},
		[]int8{},
	)

	validateInt8History(chk, int8Store, "key2", 0, // advances to clkNano2
		[]string{},
		[]int8{},
	)

	chk.NoErr(int8Store.Update("key1", 20))  // clkNano4
	chk.NoErr(int8Store.Update("key2", -20)) // clkNano5

	validateInt8History(chk, int8Store, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]int8{20},
	)

	validateInt8History(chk, int8Store, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]int8{-20},
	)

	chk.NoErr(int8Store.Delete("key1")) // clkNano8
	chk.NoErr(int8Store.Delete("key2")) // clkNano9

	validateInt8History(chk, int8Store, "key1", 0, // advances to clkNano10
		[]string{},
		[]int8{},
	)

	validateInt8History(chk, int8Store, "key2", 0, // advances to clkNano11
		[]string{},
		[]int8{},
	)

	chk.NoErr(int8Store.Update("key1", 22))  // clkNano12
	chk.NoErr(int8Store.Update("key2", -22)) // clkNano13

	validateInt8History(chk, int8Store, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]int8{22},
	)

	validateInt8History(chk, int8Store, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]int8{-22},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseInt8: invalid syntax: "abc"`,
		`parseInt8: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt8: invalid range: "128"`,
		`parseInt8: invalid range: "128"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|128"`,
		`parseInt8: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt8: invalid range: "128"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|128"`,
		`get("key1"): unknown data key`,
		`parseInt8: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseInt8: invalid range: "128"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|128"`,
		`parseInt8: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt8: invalid range: "128"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|128"`,
	)
}
