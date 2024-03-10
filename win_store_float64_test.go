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

func setupWStoreFloat64WithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreFloat64) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dir := chk.CreateTmpDir()

	const fName = "dataFile"

	s := NewFloat64(dir, fName)
	s.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dir)
	chk.AddSub("{{file}}", fName)

	return dir, fName, s
}

func validateFloat64History(
	chk *sztest.Chk,
	s *WStoreFloat64,
	datKey string,
	days uint,
	expTSlice []string,
	expVSlice []float64,
) {
	chk.T().Helper()

	ts, v, ok := s.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(ts.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Float64(v, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := s.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.Float64Slice(vSlice, expVSlice, 0)
}

func Test_WStoreFloat64_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()
	dir, file, s := setupWStoreFloat64WithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dir, file, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|1.7E+309"},
		}),
	)

	chk.NoErr(s.Open())
	defer closeAndLogIfError(s)

	validateFloat64History(chk, s, "key1", 0, // advances to clkNano2
		[]string{},
		[]float64{},
	)

	validateFloat64History(chk, s, "key2", 0, // advances to clkNano2
		[]string{},
		[]float64{},
	)

	chk.NoErr(s.Update("key1", 200.0))  // clkNano4
	chk.NoErr(s.Update("key2", -200.0)) // clkNano5

	validateFloat64History(chk, s, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]float64{200.0},
	)

	validateFloat64History(chk, s, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]float64{-200.0},
	)

	chk.NoErr(s.Delete("key1")) // clkNano8
	chk.NoErr(s.Delete("key2")) // clkNano9

	validateFloat64History(chk, s, "key1", 0, // advances to clkNano10
		[]string{},
		[]float64{},
	)

	validateFloat64History(chk, s, "key2", 0, // advances to clkNano11
		[]string{},
		[]float64{},
	)

	chk.NoErr(s.Update("key1", 222.0))  // clkNano12
	chk.NoErr(s.Update("key2", -222.0)) // clkNano13

	validateFloat64History(chk, s, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]float64{222.0},
	)

	validateFloat64History(chk, s, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]float64{-222.0},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseFloat64: invalid syntax: "abc"`,
		`parseFloat64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseFloat64: invalid range: "1.7E+309"`,
		`parseFloat64: invalid range: "1.7E+309"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|1.7E+309"`,
		`parseFloat64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseFloat64: invalid range: "1.7E+309"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|1.7E+309"`,
		`get("key1"): unknown data key`,
		`parseFloat64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseFloat64: invalid range: "1.7E+309"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|1.7E+309"`,
		`parseFloat64: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseFloat64: invalid range: "1.7E+309"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|1.7E+309"`,
	)
}
