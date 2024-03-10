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
	"strings"
	"testing"
	"time"

	"github.com/dancsecs/sztest"
)

func TestWindowStorePublic_UnknownDuplicateWindows(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	winDB := newWinDB("datKey")

	chk.NoErr(winDB.addWindow("winKey1", time.Second*1))

	chk.Err(
		winDB.addWindow("winKey1", time.Second*1),
		ErrDupWinKey.Error(),
	)

	average, err := winDB.getAvg("winKey1")
	chk.Err(
		err,
		ErrNoWinData.Error(),
	)
	chk.Float64(average, 0, 0)

	count, err := winDB.getCount("winKey1")
	chk.Err(
		err,
		ErrNoWinData.Error(),
	)
	chk.Uint64(count, 0, 0)

	average, err = winDB.getAvg("unknownWindowKey")
	chk.Err(
		err, ErrUnknownWinKey.Error(),
	)
	chk.Float64(average, 0, 0)

	count, err = winDB.getCount("unknownWindowKey")
	chk.Err(
		err, ErrUnknownWinKey.Error(),
	)
	chk.Uint64(count, 0, 0)
}

func TestWindowStorePublic_AddWindowValueUseCase(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2023, 11, 25, 10, 11, 12, 555, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	winDB := newWinDB("datKey")

	chk.NoErr(winDB.addWindow("winKey1", time.Second*1))
	chk.NoErr(winDB.addWindow("winKey2", time.Second*2))
	chk.NoErr(winDB.addWindow("winKey3", time.Second*3))
	chk.NoErr(winDB.addWindow("winKey4", time.Second*4))
	chk.NoErr(winDB.addWindow("winKey5", time.Second*5))
	chk.NoErr(winDB.addWindow("winKey6", time.Second*6))

	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: <nil> Last: <nil> Active: 0 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
		},
	)

	winDB.addValue(chk.ClockNext(), 2)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano0}} - 2 Last: {{clkNano0}} - 2 " +
				"Active: 1 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano0}} - 2 Oldest: {{clkNano0}} - 2 " +
				"Count: 1 Avg: 2", // 2 = 2 / 1 = 2
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano0}} - 2 Oldest: {{clkNano0}} - 2 " +
				"Count: 1 Avg: 2", // 2 = 2 / 1 = 2
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano0}} - 2 Oldest: {{clkNano0}} - 2 " +
				"Count: 1 Avg: 2", // 2 = 2 / 1 = 2
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano0}} - 2 Oldest: {{clkNano0}} - 2 " +
				"Count: 1 Avg: 2", // 2 = 2 / 1 = 2
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano0}} - 2 Oldest: {{clkNano0}} - 2 " +
				"Count: 1 Avg: 2", // 2 = 2 / 1 = 2
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano0}} - 2 Oldest: {{clkNano0}} - 2 " +
				"Count: 1 Avg: 2", // 2 = 2 / 1 = 2
		},
	)

	winDB.addValue(chk.ClockNext(), 4)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano1}} - 4 Last: {{clkNano0}} - 2 " +
				"Active: 2 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano1}} - 4 Oldest: {{clkNano0}} - 2 " +
				"Count: 2 Avg: 3", // 4 + 2 = 6 / 2 = 3
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano1}} - 4 Oldest: {{clkNano0}} - 2 " +
				"Count: 2 Avg: 3", // 4 + 2 = 6 / 2 = 3
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano1}} - 4 Oldest: {{clkNano0}} - 2 " +
				"Count: 2 Avg: 3", // 4 + 2 = 6 / 2 = 3
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano1}} - 4 Oldest: {{clkNano0}} - 2 " +
				"Count: 2 Avg: 3", // 4 + 2 = 6 / 2 = 3
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano1}} - 4 Oldest: {{clkNano0}} - 2 " +
				"Count: 2 Avg: 3", // 4 + 2 = 6 / 2 = 3
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano1}} - 4 Oldest: {{clkNano0}} - 2 " +
				"Count: 2 Avg: 3", // 4 + 2 = 6 / 2 = 3
		},
	)

	winDB.addValue(chk.ClockNext(), 6)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano2}} - 6 Last: {{clkNano0}} - 2 " +
				"Active: 3 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano2}} - 6 Oldest: {{clkNano1}} - 4 " +
				"Count: 2 Avg: 5", // 6 + 4 = 10 / 2 = 5
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano2}} - 6 Oldest: {{clkNano0}} - 2 " +
				"Count: 3 Avg: 4", // 6 + 4 + 2 = 12 / 3 = 4
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano2}} - 6 Oldest: {{clkNano0}} - 2 " +
				"Count: 3 Avg: 4", // 6 + 4 + 2 = 12 / 3 = 4
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano2}} - 6 Oldest: {{clkNano0}} - 2 " +
				"Count: 3 Avg: 4", // 6 + 4 + 2 = 12 / 3 = 4
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano2}} - 6 Oldest: {{clkNano0}} - 2 " +
				"Count: 3 Avg: 4", // 6 + 4 + 2 = 12 / 3 = 4
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano2}} - 6 Oldest: {{clkNano0}} - 2 " +
				"Count: 3 Avg: 4", // 6 + 4 + 2 = 12 / 3 = 4
		},
	)

	winDB.addValue(chk.ClockNext(), 8)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano3}} - 8 Last: {{clkNano0}} - 2 " +
				"Active: 4 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano3}} - 8 Oldest: {{clkNano2}} - 6 " +
				"Count: 2 Avg: 7", // 8 + 6 = 14 / 2 = 7
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano3}} - 8 Oldest: {{clkNano1}} - 4 " +
				"Count: 3 Avg: 6", // 8 + 6 + 4 = 18 / 3 = 6
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano3}} - 8 Oldest: {{clkNano0}} - 2 " +
				"Count: 4 Avg: 5", // 8 + 6 + 4 + 2 = 20 / 4 = 5
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano3}} - 8 Oldest: {{clkNano0}} - 2 " +
				"Count: 4 Avg: 5", // 8 + 6 + 4 + 2 = 20 / 4 = 5
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano3}} - 8 Oldest: {{clkNano0}} - 2 " +
				"Count: 4 Avg: 5", // 8 + 6 + 4 + 2 = 20 / 4 = 5
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano3}} - 8 Oldest: {{clkNano0}} - 2 " +
				"Count: 4 Avg: 5", // 8 + 6 + 4 + 2 = 20 / 4 = 5
		},
	)

	winDB.addValue(chk.ClockNext(), 10)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano4}} - 10 Last: {{clkNano0}} - 2 " +
				"Active: 5 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano4}} - 10 Oldest: {{clkNano3}} - 8 " +
				"Count: 2 Avg: 9", // 10 + 8 = 18 / 2 = 9
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano4}} - 10 Oldest: {{clkNano2}} - 6 " +
				"Count: 3 Avg: 8", // 10 + 8 + 6 = 24 / 3 = 8
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano4}} - 10 Oldest: {{clkNano1}} - 4 " +
				"Count: 4 Avg: 7", // 10 + 8 + 6 + 4 = 28 / 4 = 7
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano4}} - 10 Oldest: {{clkNano0}} - 2 " +
				"Count: 5 Avg: 6", // 10 + 8 + 6 + 4 + 2 = 30 / 5 = 6
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano4}} - 10 Oldest: {{clkNano0}} - 2 " +
				"Count: 5 Avg: 6", // 10 + 8 + 6 + 4 + 2 = 30 / 5 = 6
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano4}} - 10 Oldest: {{clkNano0}} - 2 " +
				"Count: 5 Avg: 6", // 10 + 8 + 6 + 4 + 2 = 30 / 5 = 6
		},
	)

	winDB.addValue(chk.ClockNext(), 12)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano5}} - 12 Last: {{clkNano0}} - 2 " +
				"Active: 6 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano5}} - 12 Oldest: {{clkNano4}} - 10 " +
				"Count: 2 Avg: 11", // 12 + 10 = 22 / 2 = 11
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano5}} - 12 Oldest: {{clkNano3}} - 8 " +
				"Count: 3 Avg: 10", // 12 + 10 + 8 = 30 / 3 = 10
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano5}} - 12 Oldest: {{clkNano2}} - 6 " +
				"Count: 4 Avg: 9", // 12 + 10 + 8 + 6 = 36 / 4 = 9
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano5}} - 12 Oldest: {{clkNano1}} - 4 " +
				"Count: 5 Avg: 8", // 12 + 10 + 8 + 6 + 4 = 40 / 5 = 8
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano5}} - 12 Oldest: {{clkNano0}} - 2 " +
				"Count: 6 Avg: 7", // 12 + 10 + 8 + 6 + 4 + 2 = 42 / 6 = 7
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano5}} - 12 Oldest: {{clkNano0}} - 2 " +
				"Count: 6 Avg: 7", // 12 + 10 + 8 + 6 + 4 + 2 = 42 / 6 = 7
		},
	)

	winDB.addValue(chk.ClockNext(), 14)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano6}} - 14 Last: {{clkNano0}} - 2 " +
				"Active: 7 Cached: 0",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano6}} - 14 Oldest: {{clkNano5}} - 12 " +
				"Count: 2 Avg: 13", // 14 + 12 = 26 / 2 = 13
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano6}} - 14 Oldest: {{clkNano4}} - 10 " +
				"Count: 3 Avg: 12", // 14 + 12 + 10 = 36 / 3 = 12
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano6}} - 14 Oldest: {{clkNano3}} - 8 " +
				"Count: 4 Avg: 11", // 14 + 12 + 10 + 8 = 44 / 4 = 11
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano6}} - 14 Oldest: {{clkNano2}} - 6 " +
				"Count: 5 Avg: 10", // 14 + 12 + 10 + 8 + 6 = 50 / 5 = 10
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano6}} - 14 Oldest: {{clkNano1}} - 4 " +
				"Count: 6 Avg: 9", // 14 + 12 + 10 + 8 + 6 + 4 = 54 / 6 = 9
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano6}} - 14 Oldest: {{clkNano0}} - 2 " +
				"Count: 7 Avg: 8", // 14 + 12 + 10 + 8 + 6 + 4 + 2 = 56 / 7 = 8
		},
	)

	winDB.addValue(chk.ClockNext(), 16)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano7}} - 16 Last: {{clkNano1}} - 4 " +
				"Active: 7 Cached: 1",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano7}} - 16 Oldest: {{clkNano6}} - 14 " +
				"Count: 2 Avg: 15", // 16 + 14 = 30 / 2 = 15
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano7}} - 16 Oldest: {{clkNano5}} - 12 " +
				"Count: 3 Avg: 14", // 16 + 14 + 12 = 42 / 3 = 14
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano7}} - 16 Oldest: {{clkNano4}} - 10 " +
				"Count: 4 Avg: 13", // 16 + 14 + 12 + 10 = 52 / 4 = 13
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano7}} - 16 Oldest: {{clkNano3}} - 8 " +
				"Count: 5 Avg: 12", // 16 + 14 + 12 + 10 + 8 = 60 / 5 = 12
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano7}} - 16 Oldest: {{clkNano2}} - 6 " +
				"Count: 6 Avg: 11", // 16 + 14 + 12 + 10 + 8 + 6 = 66 / 6 = 10
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano7}} - 16 Oldest: {{clkNano1}} - 4 " +
				"Count: 7 Avg: 10", // 16 + 14 + 12 + 10 + 8 + 6 + 4 = 70 / 7 = 10
		},
	)

	winDB.addValue(chk.ClockNext(), 18)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano8}} - 18 Last: {{clkNano2}} - 6 " +
				"Active: 7 Cached: 1",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano7}} - 16 " +
				"Count: 2 Avg: 17", // 18 + 16 = 34 / 2 = 17
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano6}} - 14 " +
				"Count: 3 Avg: 16", // 18 + 16 + 14 = 48 / 3 = 16
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano5}} - 12 " +
				"Count: 4 Avg: 15", // 18 + 16 + 14 + 12 = 60 / 4 = 15
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano4}} - 10 " +
				"Count: 5 Avg: 14", // 18 + 16 + 14 + 12 + 10 = 70 / 5 = 12
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano3}} - 8 " +
				"Count: 6 Avg: 13", // 18 + 16 + 14 + 12 + 10 + 8 = 78 / 6 = 13
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano2}} - 6 " +
				"Count: 7 Avg: 12", // 18 + 16 + 14 + 12 + 10 + 8 + 6 = 84 / 7 = 12
		},
	)

	// try to just delete one window.
	winDB.windows["winKey4"].delete()
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano8}} - 18 Last: {{clkNano2}} - 6 " +
				"Active: 7 Cached: 1",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano7}} - 16 " +
				"Count: 2 Avg: 17", // 18 + 16 = 34 / 2 = 17
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano6}} - 14 " +
				"Count: 3 Avg: 16", // 18 + 16 + 14 = 48 / 3 = 16
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano5}} - 12 " +
				"Count: 4 Avg: 15", // 18 + 16 + 14 + 12 = 60 / 4 = 15
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano3}} - 8 " +
				"Count: 6 Avg: 13", // 18 + 16 + 14 + 12 + 10 + 8 = 78 / 6 = 13
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano8}} - 18 Oldest: {{clkNano2}} - 6 " +
				"Count: 7 Avg: 12", // 18 + 16 + 14 + 12 + 10 + 8 + 6 = 84 / 7 = 12
		},
	)

	winDB.addValue(chk.ClockNext(), 20)
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: {{clkNano9}} - 20 Last: {{clkNano3}} - 8 " +
				"Active: 7 Cached: 1",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: {{clkNano9}} - 20 Oldest: {{clkNano8}} - 18 " +
				"Count: 2 Avg: 19", // 20+ 18 = 38 / 2 = 19
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: {{clkNano9}} - 20 Oldest: {{clkNano7}} - 16 " +
				"Count: 3 Avg: 18", // 20 + 18 + 16 + 14 = 54 / 3 = 18
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: {{clkNano9}} - 20 Oldest: {{clkNano6}} - 14 " +
				"Count: 4 Avg: 17", // 20 + 18 + 16 + 14 + 12 = 68 / 4 = 17
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: {{clkNano9}} - 20 Oldest: {{clkNano9}} - 20 " +
				"Count: 1 Avg: 20", // 20 = 20 / 1 = 20
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: {{clkNano9}} - 20 Oldest: {{clkNano4}} - 10 " +
				"Count: 6 Avg: 15", // 20 + 18 + 16 + 14 + 12 + 10  = 90 / 6 = 15
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: {{clkNano9}} - 20 Oldest: {{clkNano3}} - 8 " +
				"Count: 7 Avg: 14", // 20 + 18 + 16 + 14 + 12 + 10 + 8  = 98 / 7 = 14
		},
	)
	winDB.delete()
	chk.StrSlice(
		strings.Split(winDB.String(), "\n"), []string{
			"winDB: datKey: datKey maxPeriod: 6s",
			"\tFirst: <nil> Last: <nil> Active: 0 Cached: 8",
			"\t\tdatKey: datKey winKey: winKey1 Period: 1s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey2 Period: 2s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey3 Period: 3s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey4 Period: 4s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey5 Period: 5s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
			"\t\tdatKey: datKey winKey: winKey6 Period: 6s " +
				"Newest: <nil> Oldest: <nil> " +
				"Count: 0 Avg: 0",
		},
	)
}

func TestWindowWindows_Thresholds(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	chk.ClockSet(
		time.Date(2023, 11, 25, 10, 11, 12, 555, time.Local),
		time.Second,
	)
	chk.ClockAddSub(sztest.ClockSubNano)

	winDB := newWinDB("datKey")

	chk.NoErr(winDB.addWindow("winKey1", time.Second*1))
	chk.NoErr(winDB.addWindow("winKey2", time.Second*2))

	chk.Err(
		winDB.addThreshold("unknownWinKey", 2, 4, 6, 8,
			func(_, _ string, _, _ ThresholdReason, _ float64) {
			}),
		ErrUnknownWinKey.Error(),
	)

	callback1Triggered := false
	callback2Triggered := false

	chk.NoErr(
		winDB.addThreshold("winKey1", 1, 3, 5, 7,
			func(_, _ string, _, t ThresholdReason, avg float64) {
				callback1Triggered = avg == 100.0 && t == ThresholdHighCritical
			},
		),
	)

	chk.NoErr(
		winDB.addThreshold("winKey2", 1, 3, 5, 7,
			func(_, _ string, _, t ThresholdReason, avg float64) {
				callback2Triggered = avg == 100.0 && t == ThresholdHighCritical
			},
		),
	)

	winDB.addValue(chk.ClockNext(), 100.0)

	chk.True(callback1Triggered)
	chk.True(callback2Triggered)
}
