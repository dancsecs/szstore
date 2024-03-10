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

func setupWStoreUint16WithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreUint16) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	uint16Store := NewUint16(dirName, filename)
	uint16Store.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, uint16Store
}

func validateUint16History(
	chk *sztest.Chk,
	uint16Store *WStoreUint16,
	datKey string,
	days uint,
	expTSlice []string,
	expVSlice []uint16,
) {
	chk.T().Helper()

	timestamp, value, ok := uint16Store.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Uint16(value, expVSlice[len(expVSlice)-1], 0)
	}

	tsSlice, vSlice := uint16Store.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.Uint16Slice(vSlice, expVSlice, 0)
}

func Test_WStoreUint16_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, uint16Store := setupWStoreUint16WithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|abc"},
			{ /* clkNano1 */ "", "|U|key2|65536"},
		}),
	)

	chk.NoErr(uint16Store.Open())
	defer closeAndLogIfError(uint16Store)

	validateUint16History(chk, uint16Store, "key1", 0, // advances to clkNano2
		[]string{},
		[]uint16{},
	)

	validateUint16History(chk, uint16Store, "key2", 0, // advances to clkNano2
		[]string{},
		[]uint16{},
	)

	chk.NoErr(uint16Store.Update("key1", 200)) // clkNano4
	chk.NoErr(uint16Store.Update("key2", 400)) // clkNano5

	validateUint16History(chk, uint16Store, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]uint16{200},
	)

	validateUint16History(chk, uint16Store, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]uint16{400},
	)

	chk.NoErr(uint16Store.Delete("key1")) // clkNano8
	chk.NoErr(uint16Store.Delete("key2")) // clkNano9

	validateUint16History(chk, uint16Store, "key1", 0, // advances to clkNano10
		[]string{},
		[]uint16{},
	)

	validateUint16History(chk, uint16Store, "key2", 0, // advances to clkNano11
		[]string{},
		[]uint16{},
	)

	chk.NoErr(uint16Store.Update("key1", 222)) // clkNano12
	chk.NoErr(uint16Store.Update("key2", 444)) // clkNano13

	validateUint16History(chk, uint16Store, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]uint16{222},
	)

	validateUint16History(chk, uint16Store, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]uint16{444},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseUint16: invalid syntax: "abc"`,
		`parseUint16: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint16: invalid range: "65536"`,
		`parseUint16: invalid range: "65536"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|65536"`,
		`parseUint16: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint16: invalid range: "65536"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|65536"`,
		`get("key1"): unknown data key`,
		`parseUint16: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`get("key2"): unknown data key`,
		`parseUint16: invalid range: "65536"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|65536"`,
		`parseUint16: invalid syntax: "abc"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|abc"`,
		`parseUint16: invalid range: "65536"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|65536"`,
	)
}
