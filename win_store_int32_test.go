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

func setupWStoreInt32WithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreInt32) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	int32Store := NewInt32(dirName, filename)
	int32Store.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, int32Store
}

func validateInt32History(
	chk *sztest.Chk,
	int32Store *WStoreInt32,
	datKey string,
	days uint,
	expTSlice []string,
	expVSlice []int32,
) {
	chk.T().Helper()

	timestamp, values, ok := int32Store.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Int32(values, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := int32Store.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.Int32Slice(vSlice, expVSlice, 0)
}

func Test_WStoreInt32_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, int32Store := setupWStoreInt32WithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|2147483648"},
		}),
	)

	chk.NoErr(int32Store.Open())
	defer closeAndLogIfError(int32Store)

	validateInt32History(chk, int32Store, "key1", 0, // advances to clkNano2
		[]string{},
		[]int32{},
	)

	validateInt32History(chk, int32Store, "key2", 0, // advances to clkNano2
		[]string{},
		[]int32{},
	)

	chk.NoErr(int32Store.Update("key1", 200))  // clkNano4
	chk.NoErr(int32Store.Update("key2", -200)) // clkNano5

	validateInt32History(chk, int32Store, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]int32{200},
	)

	validateInt32History(chk, int32Store, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]int32{-200},
	)

	chk.NoErr(int32Store.Delete("key1")) // clkNano8
	chk.NoErr(int32Store.Delete("key2")) // clkNano9

	validateInt32History(chk, int32Store, "key1", 0, // advances to clkNano10
		[]string{},
		[]int32{},
	)

	validateInt32History(chk, int32Store, "key2", 0, // advances to clkNano11
		[]string{},
		[]int32{},
	)

	chk.NoErr(int32Store.Update("key1", 222))  // clkNano12
	chk.NoErr(int32Store.Update("key2", -222)) // clkNano13

	validateInt32History(chk, int32Store, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]int32{222},
	)

	validateInt32History(chk, int32Store, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]int32{-222},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseInt32: invalid syntax: "abc"`,
		`parseInt32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt32: invalid range: "2147483648"`,
		`parseInt32: invalid range: "2147483648"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|2147483648"`,
		`parseInt32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt32: invalid range: "2147483648"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|2147483648"`,
		`get("key1"): unknown data key`,
		`parseInt32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseInt32: invalid range: "2147483648"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|2147483648"`,
		`parseInt32: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt32: invalid range: "2147483648"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|2147483648"`,
	)
}
