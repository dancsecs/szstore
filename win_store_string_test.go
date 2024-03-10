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

func setupWStoreStringWithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *WStoreString) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	dirName := chk.CreateTmpDir()

	const filename = "dataFile"

	stringStore := NewString(dirName, filename)
	stringStore.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, stringStore
}

func validateStringHistory(
	chk *sztest.Chk,
	stringStore *WStoreString,
	datKey string,
	days uint, //nolint:unparam // Always a 0.
	expTSlice, expVSlice []string,
) {
	chk.T().Helper()

	timestamp, value, ok := stringStore.Get(datKey)

	if len(expTSlice) == 0 {
		chk.Falsef(ok, "Checking s.Get(%q)", datKey)
	} else {
		chk.True(ok)
		chk.Str(timestamp.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
		chk.Str(value, expVSlice[len(expVSlice)-1])
	}

	tsSlice, vSlice := stringStore.GetHistoryDays(datKey, days)

	var tSlice []string

	for _, ts := range tsSlice {
		tSlice = append(tSlice, ts.Format(fmtTimeStamp))
	}

	chk.StrSlice(tSlice, expTSlice)
	chk.StrSlice(vSlice, expVSlice)
}

func Test_WStoreString_InvalidStringContent(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, stringStore := setupWStoreStringWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|"},
		}),
	)

	stringStore.SetInvalidChars([]rune{'<', '>'})

	chk.NoErr(stringStore.Open())
	defer closeAndLogIfError(stringStore)

	chk.Err(
		stringStore.Update("invalidContent", "<"),
		ErrInvalidStoreString.Error(),
	)

	chk.NoErr(stringStore.Update("valueContent", "a"))

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseString: invalid character: "<"`,
	)
}

func Test_WStoreString_InvalidStringValue(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, fileName, stringStore := setupWStoreStringWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, fileName, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|"},
		}),
	)

	stringStore.SetValidValues([]string{"one", "two"})

	chk.NoErr(stringStore.Open())
	defer closeAndLogIfError(stringStore)

	chk.Err(
		stringStore.Update("invalidContent", "three"),
		ErrInvalidStoreString.Error(),
	)

	chk.NoErr(stringStore.Update("invalidContent", "two"))

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseString: invalid value: "three"`,
	)
}

func Test_WStoreString_UseCase(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, stringStore := setupWStoreStringWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Millisecond*20,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0 */ "", "|U|key1|<"},
			{ /* clkNano1 */ "", "|U|key2|>"},
		}),
	)

	stringStore.SetInvalidChars([]rune{'<', '>'})

	chk.NoErr(stringStore.Open())
	defer closeAndLogIfError(stringStore)

	validateStringHistory(chk, stringStore, "key1", 0, // advances to clkNano2
		[]string{},
		[]string{},
	)

	validateStringHistory(chk, stringStore, "key2", 0, // advances to clkNano2
		[]string{},
		[]string{},
	)

	chk.NoErr(stringStore.Update("key1", "key1BeforeDelete")) // clkNano4
	chk.NoErr(stringStore.Update("key2", "key2BeforeDelete")) // clkNano5

	validateStringHistory(chk, stringStore, "key1", 0, // advances to clkNano6
		[]string{"{{clkNano4}}"},
		[]string{"key1BeforeDelete"},
	)

	validateStringHistory(chk, stringStore, "key2", 0, // advances to clkNano7
		[]string{"{{clkNano5}}"},
		[]string{"key2BeforeDelete"},
	)

	chk.NoErr(stringStore.Delete("key1")) // clkNano8
	chk.NoErr(stringStore.Delete("key2")) // clkNano9

	validateStringHistory(chk, stringStore, "key1", 0, // advances to clkNano10
		[]string{},
		[]string{},
	)

	validateStringHistory(chk, stringStore, "key2", 0, // advances to clkNano11
		[]string{},
		[]string{},
	)

	chk.NoErr(stringStore.Update("key1", "key1AfterDelete")) // clkNano12
	chk.NoErr(stringStore.Update("key2", "key2AfterDelete")) // clkNano13

	validateStringHistory(chk, stringStore, "key1", 0, // advances to clkNano14
		[]string{"{{clkNano12}}"},
		[]string{"key1AfterDelete"},
	)

	validateStringHistory(chk, stringStore, "key2", 0, // advances to clkNano15
		[]string{"{{clkNano13}}"},
		[]string{"key2AfterDelete"},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`parseString: invalid character: "<"`,
		`parseString: invalid character: "<"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|<"`,
		`parseString: invalid character: ">"`,
		`parseString: invalid character: ">"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|>"`,
		`parseString: invalid character: "<"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|<"`,
		`parseString: invalid character: ">"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|>"`,
		`get("key1"): unknown data key`,
		`parseString: invalid character: "<"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|<"`,
		`get("key2"): unknown data key`,
		`parseString: invalid character: ">"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|>"`,
		`parseString: invalid character: "<"`+
			`: {{hPath0}}:1 - "{{clkNano0}}|U|key1|<"`,
		`parseString: invalid character: ">"`+
			`: {{hPath0}}:2 - "{{clkNano1}}|U|key2|>"`,
	)
}
