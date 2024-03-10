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

func setupWStoreInt64WithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreInt64) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	int64Store := NewInt64(dirName, filename)
	int64Store.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, int64Store
}

func validateInt64History(
	chk *sztest.Chk,
	int64Store *WStoreInt64,
	datKey string,
	days uint, //nolint:unparam // Always a 0.
	expTSlice []string,
	expVSlice []int64,
) {
	chk.T().Helper()

	timestamp, value, ok := int64Store.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Int64(value, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := int64Store.GetHistoryDays(datKey, days)

	tSlice := make([]string, len(tsSlice))
	for i, ts := range tsSlice {
		tSlice[i] = ts.Format(fmtTimeStamp)
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.Int64Slice(vSlice, expVSlice, 0)
}

//nolint:funlen // Ok.
func Test_WStoreInt64_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, int64Store := setupWStoreInt64WithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|9223372036854775808"},
		}),
	)

	chk.NoErr(int64Store.Open())
	defer closeAndLogIfError(int64Store)

	validateInt64History(chk, int64Store, "key1", 0, // advances to clkNano2
		[]string{},
		[]int64{},
	)

	validateInt64History(chk, int64Store, "key2", 0, // advances to clkNano2
		[]string{},
		[]int64{},
	)

	chk.NoErr(int64Store.Update("key1", 200))  // clkNano4
	chk.NoErr(int64Store.Update("key2", -200)) // clkNano5

	validateInt64History(chk, int64Store, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]int64{200},
	)

	validateInt64History(chk, int64Store, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]int64{-200},
	)

	chk.NoErr(int64Store.Delete("key1")) // clkNano8
	chk.NoErr(int64Store.Delete("key2")) // clkNano9

	validateInt64History(chk, int64Store, "key1", 0, // advances to clkNano10
		[]string{},
		[]int64{},
	)

	validateInt64History(chk, int64Store, "key2", 0, // advances to clkNano11
		[]string{},
		[]int64{},
	)

	chk.NoErr(int64Store.Update("key1", 222))  // clkNano12
	chk.NoErr(int64Store.Update("key2", -222)) // clkNano13

	validateInt64History(chk, int64Store, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]int64{222},
	)

	validateInt64History(chk, int64Store, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]int64{-222},
	)

	chk.Log(
		` opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseInt64: invalid syntax: "abc"`,
		`parseInt64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt64: invalid range: "9223372036854775808"`,
		`parseInt64: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
		`parseInt64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt64: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
		`get("key1"): unknown data key`,
		`parseInt64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseInt64: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
		`parseInt64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt64: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
	)
}
