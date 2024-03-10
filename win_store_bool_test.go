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
	"log"
	"testing"
	"time"

	"github.com/dancsecs/sztest"
)

func TestSzStoreBool_InvalidBoolThresholds(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dir, file, s := setupWStoreBoolWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dir, file, [][2]string{
			{"", "|U|key2|false"},
			{"", "|U|key2|true"},
		}),
	)

	chk.NoErr(
		s.AddWindow("key2", "18Milliseconds", time.Millisecond*18),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 1.2, 0.4, 0.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, 1.4, 0.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, 0.4, 1.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, 0.4, 0.6, 1.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", -0.2, 0.4, 0.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, -0.4, 0.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, 0.4, -0.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, 0.4, 0.6, -0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidBoolThreshold.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.4, 0.2, 0.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidThresholdOrder.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, 0.6, 0.4, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidThresholdOrder.Error(),
	)

	chk.Err(
		s.AddWindowThreshold("key2", "18Milliseconds", 0.2, 0.4, 0.8, 0.6,
			func(k, w string, o, n ThresholdReason, v float64) {
			},
		),
		ErrInvalidThresholdOrder.Error(),
	)

	chk.NoErr(s.Open())
	defer closeAndLogIfError(s)

	chk.Log(
		"opening file based szStore {{file}} in directory {{dir}}",
		"starting path retrieved as: {{hPath0}}",
	)
}

func setupWStoreBoolWithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreBool) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dir := chk.CreateTmpDir()

	const fName = "dataFile"

	s := NewBool(dir, fName)
	s.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dir)
	chk.AddSub("{{file}}", fName)

	return dir, fName, s
}

func validateBoolHistory(
	chk *sztest.Chk,
	s *WStoreBool,
	datKey string,
	days uint,
	expTSlice []string,
	expVSlice []bool,
) {
	chk.T().Helper()

	ts, v, ok := s.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(ts.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Bool(v, expVSlice[len(expVSlice)-1], 0)
	}
	tsSlice, vSlice := s.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}
	chk.StrSlice(tSlice, expTSlice)
	chk.BoolSlice(vSlice, expVSlice, 0)
}

func Test_WStoreBool_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()
	dir, file, s := setupWStoreBoolWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dir, file, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|def"},
		}),
	)

	chk.NoErr(s.Open())
	defer closeAndLogIfError(s)

	validateBoolHistory(chk, s, "key1", 0, // advances to clkNano2
		[]string{},
		[]bool{},
	)

	validateBoolHistory(chk, s, "key2", 0, // advances to clkNano2
		[]string{},
		[]bool{},
	)

	chk.NoErr(s.Update("key1", true))  // clkNano4
	chk.NoErr(s.Update("key2", false)) // clkNano5

	validateBoolHistory(chk, s, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]bool{true},
	)

	validateBoolHistory(chk, s, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]bool{false},
	)

	chk.NoErr(s.Delete("key1")) // clkNano8
	chk.NoErr(s.Delete("key2")) // clkNano9

	validateBoolHistory(chk, s, "key1", 0, // advances to clkNano10
		[]string{},
		[]bool{},
	)

	validateBoolHistory(chk, s, "key2", 0, // advances to clkNano11
		[]string{},
		[]bool{},
	)

	chk.NoErr(s.Update("key1", false)) // clkNano12
	chk.NoErr(s.Update("key2", true))  // clkNano13

	validateBoolHistory(chk, s, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]bool{false},
	)

	validateBoolHistory(chk, s, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]bool{true},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseBool: invalid syntax: "abc"`,
		`parseBool: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseBool: invalid syntax: "def"`,
		`parseBool: invalid syntax: "def"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|def"`,
		`parseBool: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseBool: invalid syntax: "def"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|def"`,
		`get("key1"): unknown data key`,
		`parseBool: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseBool: invalid syntax: "def"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|def"`,
		`parseBool: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseBool: invalid syntax: "def"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|def"`,
	)
}

func TestSzStoreBool_UseCase3(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dir, file, boolStore := setupWStoreBoolWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*3,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dir, file, [][2]string{
			{"", "|U|key1|false"},
		}),
	)

	chk.NoErr(
		boolStore.AddWindow("key1", "20Milliseconds", time.Millisecond*20),
	)

	chk.NoErr(
		boolStore.AddWindow("key2", "18Milliseconds", time.Millisecond*18),
	)

	chk.Err(
		boolStore.AddWindow("key2", "18Milliseconds", time.Millisecond*18),
		ErrDupWinKey.Error(),
	)

	chk.NoErr(
		boolStore.AddWindowThreshold("key2", "18Milliseconds", 0.2, 0.4, 0.6, 0.8,
			func(k, w string, o, n ThresholdReason, v float64) {
				log.Printf("Threshold change for key: %s window: %s old: %20s, new %20s for: %f",
					k, w, o, n, v)
			},
		),
	)

	chk.NoErr(boolStore.Open())
	defer closeAndLogIfError(boolStore)

	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	_ = boolStore.Update("key2", true)
	_ = boolStore.Update("key2", false)
	chk.Log(
		"opening file based szStore {{file}} in directory {{dir}}",
		"starting path retrieved as: {{hPath0}}",
		"Threshold change for key: key2 window: 18Milliseconds old:              Unknown, new         Low Critical for: 0.000000",
		"Threshold change for key: key2 window: 18Milliseconds old:         Low Critical, new          Low Warning for: 0.285714",
		"Threshold change for key: key2 window: 18Milliseconds old:          Low Warning, new               Normal for: 0.428571",
		"Threshold change for key: key2 window: 18Milliseconds old:               Normal, new         High Warning for: 0.714286",
		"Threshold change for key: key2 window: 18Milliseconds old:         High Warning, new        High Critical for: 0.857143",
		"Threshold change for key: key2 window: 18Milliseconds old:        High Critical, new         High Warning for: 0.714286",
		"Threshold change for key: key2 window: 18Milliseconds old:         High Warning, new               Normal for: 0.571429",
		"Threshold change for key: key2 window: 18Milliseconds old:               Normal, new          Low Warning for: 0.285714",
		"Threshold change for key: key2 window: 18Milliseconds old:          Low Warning, new         Low Critical for: 0.142857",
		"Threshold change for key: key2 window: 18Milliseconds old:         Low Critical, new          Low Warning for: 0.285714",
		"Threshold change for key: key2 window: 18Milliseconds old:          Low Warning, new               Normal for: 0.428571",
	)
}
