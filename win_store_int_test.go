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

func setupWStoreIntWithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreInt) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dir := chk.CreateTmpDir()

	const fName = "dataFile"

	s := NewInt(dir, fName)
	s.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dir)
	chk.AddSub("{{file}}", fName)

	return dir, fName, s
}

func validateIntHistory(
	chk *sztest.Chk,
	s *WStoreInt,
	datKey string,
	days uint,
	expTSlice []string,
	expVSlice []int,
) {
	chk.T().Helper()

	ts, v, ok := s.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(ts.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Int(v, expVSlice[len(expVSlice)-1], 0)
	}
	tsSlice, vSlice := s.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}
	chk.StrSlice(tSlice, expTSlice)
	chk.IntSlice(vSlice, expVSlice, 0)
}

func Test_WStoreInt_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()
	dir, file, s := setupWStoreIntWithClock(
		chk,
		time.Date(2000, 05, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dir, file, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|9223372036854775808"},
		}),
	)

	chk.NoErr(s.Open())
	defer closeAndLogIfError(s)

	validateIntHistory(chk, s, "key1", 0, // advances to clkNano2
		[]string{},
		[]int{},
	)

	validateIntHistory(chk, s, "key2", 0, // advances to clkNano2
		[]string{},
		[]int{},
	)

	chk.NoErr(s.Update("key1", 200))  // clkNano4
	chk.NoErr(s.Update("key2", -200)) // clkNano5

	validateIntHistory(chk, s, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]int{200},
	)

	validateIntHistory(chk, s, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]int{-200},
	)

	chk.NoErr(s.Delete("key1")) // clkNano8
	chk.NoErr(s.Delete("key2")) // clkNano9

	validateIntHistory(chk, s, "key1", 0, // advances to clkNano10
		[]string{},
		[]int{},
	)

	validateIntHistory(chk, s, "key2", 0, // advances to clkNano11
		[]string{},
		[]int{},
	)

	chk.NoErr(s.Update("key1", 222))  // clkNano12
	chk.NoErr(s.Update("key2", -222)) // clkNano13

	validateIntHistory(chk, s, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]int{222},
	)

	validateIntHistory(chk, s, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]int{-222},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseInt: invalid syntax: "abc"`,
		`parseInt: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt: invalid range: "9223372036854775808"`,
		`parseInt: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
		`parseInt: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
		`get("key1"): unknown data key`,
		`parseInt: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseInt: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
		`parseInt: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseInt: invalid range: "9223372036854775808"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|9223372036854775808"`,
	)
}