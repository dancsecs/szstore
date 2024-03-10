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
	"strconv"
	"testing"
	"time"

	"github.com/dancsecs/sztest"
)

func TestWindowEntry_String(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	var entry *windowEntry

	chk.Strf(entry.String(), "<nil>", "unexpected return from %v", entry)

	chk.ClockSet(
		time.Date(2020, time.January, 1, 2, 3, 4, 500000000, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	entry = entry.newHead(nil, chk.ClockNext(), 123.456)
	chk.Str(entry.String(), "{{clkNano0}} - 123.456")
}

//nolint:funlen // Ok.
func TestWindowEntry_Logging(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	var (
		first *windowEntry
		last  *windowEntry
	)

	ts := time.Date(2020, time.January, 1, 2, 3, 4, 5000, time.Local)
	next := func() time.Time {
		ts = ts.Add(time.Second)

		return ts
	}
	log := func(line string) {
		if first == nil {
			log.Print(line + " FIRST: <nil>")
		} else {
			log.Print(
				line +
					" FIRST: {" + first.timestamp.Format(fmtTimeStamp) + "," +
					strconv.FormatFloat(first.value, 'g', -1, 64) + "}")
		}

		if last == nil {
			log.Print(line + "  LAST: <nil>")
		} else {
			log.Print(line +
				"  LAST: {" + last.timestamp.Format(fmtTimeStamp) + "," +
				strconv.FormatFloat(last.value, 'g', -1, 64) + "}")
		}
	}
	log("Uninitialized")

	first = first.newHead(nil, next(), 1)

	log("First Initialized with abc")

	first = first.newHead(nil, next(), 22)

	log("New first def")

	first = first.newHead(nil, next(), 333)

	log("New first ghi")

	first = first.newHead(nil, next(), 4444)

	log("New First jkl")

	first = first.newHead(nil, next(), 55555)

	log("New First mno")

	chk.Log(
		"Uninitialized FIRST: <nil>",
		"Uninitialized  LAST: <nil>",
		"First Initialized with abc FIRST: {20200101020305.000005000,1}",
		"First Initialized with abc  LAST: <nil>",
		"New first def FIRST: {20200101020306.000005000,22}",
		"New first def  LAST: <nil>",
		"New first ghi FIRST: {20200101020307.000005000,333}",
		"New first ghi  LAST: <nil>",
		"New First jkl FIRST: {20200101020308.000005000,4444}",
		"New First jkl  LAST: <nil>",
		"New First mno FIRST: {20200101020309.000005000,55555}",
		"New First mno  LAST: <nil>",
	)
}

func TestWindowWindow_New(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	newWindow := newWindow("datKey1", "winKey1", time.Second)

	chk.Str(
		newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 1s "+
			"Newest: <nil> Oldest: <nil> "+
			"Count: 0 Avg: 0",
	)
}

func TestWindowWindow_New_ZeroPeriod(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	newWindow := newWindow("datKey1", "winKey1", 0)

	chk.Str(
		newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 1ns "+
			"Newest: <nil> Oldest: <nil> "+
			"Count: 0 Avg: 0",
	)
}

func TestWindowWindow_AddFirstElement(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2020, time.January, 1, 2, 3, 4, 500000000, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	newWindow := newWindow("datKey1", "winKey1", time.Second*5)

	chk.Str(
		newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: <nil> Oldest: <nil> "+
			"Count: 0 Avg: 0",
	)

	var e *windowEntry
	e = e.newHead(nil, chk.ClockNext(), 3)
	newWindow.add(e)
	chk.Str(
		newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: {{clkNano0}} - 3 Oldest: {{clkNano0}} - 3 "+
			"Count: 1 Avg: 3",
	)
}

func TestWindowWindow_TrimFromEmpty(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	newWindow := newWindow("datKey1", "winKey1", time.Second*5)

	count, err := newWindow.getCount()
	chk.Err(err, ErrNoWinData.Error())
	chk.Uint64(count, 0)

	newWindow.trim()

	count, err = newWindow.getCount()
	chk.Err(err, ErrNoWinData.Error())
	chk.Uint64(count, 0)
}

func TestWindowWindow_TrimWithOneElement(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2020, time.January, 1, 2, 3, 4, 500000000, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	newWindow := newWindow("datKey1", "winKey1", time.Second*5)

	var e *windowEntry
	e = e.newHead(nil, chk.ClockNext(), 3)
	newWindow.add(e)

	chk.Str(
		newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: {{clkNano0}} - 3 Oldest: {{clkNano0}} - 3 "+
			"Count: 1 Avg: 3",
	)

	newWindow.trim() // Must do nothing.

	chk.Str(
		newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: {{clkNano0}} - 3 Oldest: {{clkNano0}} - 3 "+
			"Count: 1 Avg: 3",
	)
}

func TestWindowWindow_AddTwoNoTrim(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2020, time.January, 1, 2, 3, 4, 500000000, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	newWindow := newWindow("datKey1", "winKey1", time.Second*5)

	var e *windowEntry
	e = e.newHead(nil, chk.ClockNext(), 2)
	newWindow.add(e)
	e = e.newHead(nil, chk.ClockNext(), 4)
	newWindow.add(e)

	chk.Str(newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: {{clkNano1}} - 4 Oldest: {{clkNano0}} - 2 "+
			"Count: 2 Avg: 3",
	)
}

func TestWindowWindow_AddThreeNoTrim(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2020, time.January, 1, 2, 3, 4, 500000000, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	newWindow := newWindow("datKey1", "winKey1", time.Second*5)

	var entry *windowEntry
	entry = entry.newHead(nil, chk.ClockNext(), 2)
	newWindow.add(entry)
	entry = entry.newHead(nil, chk.ClockNext(), 4)
	newWindow.add(entry)
	entry = entry.newHead(nil, chk.ClockNext(), 6)
	newWindow.add(entry)

	chk.Str(newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: {{clkNano2}} - 6 Oldest: {{clkNano0}} - 2 "+
			"Count: 3 Avg: 4",
	)
}

func TestWindowWindow_AddThreeWithTrim(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2020, time.January, 1, 2, 3, 4, 500000000, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	newWindow := newWindow("datKey1", "winKey1", time.Second*1)

	var entry *windowEntry
	entry = entry.newHead(nil, chk.ClockNext(), 2)
	newWindow.add(entry)
	entry = entry.newHead(nil, chk.ClockNext(), 4)
	newWindow.add(entry)
	entry = entry.newHead(nil, chk.ClockNext(), 6)
	newWindow.add(entry)

	chk.Str(newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 1s "+
			"Newest: {{clkNano2}} - 6 Oldest: {{clkNano1}} - 4 "+
			"Count: 2 Avg: 5",
	)
}

func TestWindowWindow_AddThreeThenDelete(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2020, time.January, 1, 2, 3, 4, 500000000, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	newWindow := newWindow("datKey1", "winKey1", time.Second*5)

	var entry *windowEntry
	entry = entry.newHead(nil, chk.ClockNext(), 2)
	newWindow.add(entry)
	entry = entry.newHead(nil, chk.ClockNext(), 4)
	newWindow.add(entry)
	entry = entry.newHead(nil, chk.ClockNext(), 6)
	newWindow.add(entry)

	chk.Str(newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: {{clkNano2}} - 6 Oldest: {{clkNano0}} - 2 "+
			"Count: 3 Avg: 4",
	)

	newWindow.delete()

	chk.Str(
		newWindow.String(),
		"datKey: datKey1 winKey: winKey1 Period: 5s "+
			"Newest: <nil> Oldest: <nil> "+
			"Count: 0 Avg: 0",
	)
}

func TestWindowWindow_TestThresholds(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2023, 11, 25, 10, 11, 12, 555, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	window := newWindow("D", "W1", time.Second*5)

	callbackTriggered := false

	chk.NoErr(
		window.addThreshold(1, 3, 5, 7,
			func(_, _ string, _, t ThresholdReason, avg float64) {
				callbackTriggered = avg == 100.0 && t == ThresholdHighCritical
			},
		),
	)

	window.add(&windowEntry{
		timestamp: chk.ClockNext(),
		value:     100.0,
		next:      nil,
		prev:      nil,
	})

	chk.True(callbackTriggered)
}
