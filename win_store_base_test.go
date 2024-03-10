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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dancsecs/sztest"
)

func setupWStoreBaseWithClock(
	chk *sztest.Chk,
	initialTime time.Time, inc ...time.Duration,
) (string, string, *fileStore) {
	chk.T().Helper()

	chk.ClockSet(initialTime, inc...)
	chk.ClockAddSub(sztest.ClockSubNano)

	const filename = "dataFile"

	dirName := chk.CreateTmpDir()

	fStore := newFileStore(dirName, filename)
	// Use test clock for predictable timestamps.
	fStore.ts = chk.ClockNext

	chk.AddSub("{{dir}}", dirName)
	chk.AddSub("{{file}}", filename)

	return dirName, filename, fStore
}

func validateHistory(
	chk *sztest.Chk,
	fStore *fileStore,
	datKey string,
	days uint, //nolint:unparam // Always a 2.
	expTSlice, expVSlice []string,
) {
	chk.T().Helper()

	ts, v, ok := fStore.get(datKey)
	chk.True(ok)
	chk.Str(ts.Format(fmtTimeStamp), expTSlice[len(expTSlice)-1])
	chk.Str(v, expVSlice[len(expVSlice)-1])

	var (
		tSlice []string
		vSlice []string
	)

	fStore.getHistoryDays(datKey, days,
		func(a Action, ts time.Time, raw string) {
			if a == ActionDelete {
				tSlice = nil
				vSlice = nil
			} else {
				tSlice = append(tSlice, ts.Format(fmtTimeStamp))
				vSlice = append(vSlice, raw)
			}
		},
	)
	chk.StrSlice(tSlice, expTSlice)
	chk.StrSlice(vSlice, expVSlice)
}

func countFiles(dirName, filenameRoot string) int {
	entries, err := os.ReadDir(dirName)
	if err == nil {
		count := 0

		for _, e := range entries {
			if strings.HasPrefix(e.Name(), filenameRoot) {
				count++
			}
		}

		return count
	}

	return -1
}

func TestWStoreBase_OpenInvalidDIrectory(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	s := newFileStore("INVALID_DIRECTORY", "NO_FILE")

	chk.Err(
		s.Open(),
		"open INVALID_DIRECTORY: no such file or directory",
	)

	chk.Log(
		`opening file based szStore NO_FILE in directory INVALID_DIRECTORY`,
	)
}

func TestWStoreBase_EmptyDirectory(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	chk.Err(
		fStore.Open(),
		"",
	)

	fPath := filepath.Join(dirName, filename) +
		"_" +
		chk.ClockLastFmtDate() +
		fileExtension
	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path generated as: `+fPath,
	)
}

// Test all file parsing error logging.
//
//nolint:funlen // Ok.
func TestWStoreBase_OpenInvalidRecordParsing(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	//nolint:lll // Ok.
	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano0         */ "", "|U|key1|First"},
			{ /* clkNano1         */ "", "|U|Bad"},       // Missing field.
			{ /* clkNano2         */ "", "||key1|Bad"},   // Missing action.
			{ /* clkNano3         */ "", "|X|key1|Bad"},  // Invalid action.
			{ /* clkNano4         */ "", "|UU|key1|Bad"}, // Invalid action length.
			{ /* clkNano5         */ "", "|U||Bad"},      // Missing key.
			{ /* clkNano6         */ "", "|U|k|Bad"},     // Invalid key length.
			{"200005q5122455.500000000", "|U|key1|Bad"},  // Invalid date.
			{"20000515122q55.500000000", "|U|key1|Bad"},  // Invalid time.
			{"20000515122455.50000q000", "|U|key1|Bad"},  // Invalid time fraction.
			{"20000515122445.000000000", "|U|key1|Bad"},  // Time out of sequence.
			{"19990515122455.400000000", "|U|key1|Bad"},  // Date before file date.
			{"29990515122455.400000000", "|U|key1|Bad"},  // Date after file date.
			{ /* clkNano7         */ "", "|U|key1|Final"},
		}),
	)

	chk.NoErr(fStore.Open())
	defer closeAndLogIfError(fStore)

	validateHistory(chk, fStore, "key1", 2,
		[]string{"{{clkNano0}}", "{{clkNano7}}"},
		[]string{"First", "Final"},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`splitRecord: invalid number of fields: "3": {{hPath0}}:2`+
			` - "{{clkNano1}}|U|Bad"`,
		`splitRecord: invalid action: "": {{hPath0}}:3`+
			` - "{{clkNano2}}||key1|Bad"`,
		`splitRecord: invalid action: "X": {{hPath0}}:4`+
			` - "{{clkNano3}}|X|key1|Bad"`,
		`splitRecord: invalid action: "UU": {{hPath0}}:5`+
			` - "{{clkNano4}}|UU|key1|Bad"`,
		`splitRecord: invalid key length (>= 2 characters): "": {{hPath0}}:6`+
			` - "{{clkNano5}}|U||Bad"`,
		`splitRecord: invalid key length (>= 2 characters): "k": {{hPath0}}:7`+
			` - "{{clkNano6}}|U|k|Bad"`,
		`splitRecord: invalid date: "200005q5122455.500000000":`+
			` {{hPath0}}:8`+
			` - "200005q5122455.500000000|U|key1|Bad"`,
		`splitRecord: invalid date: "20000515122q55.500000000":`+
			` {{hPath0}}:9`+
			` - "20000515122q55.500000000|U|key1|Bad"`,
		`splitRecord: invalid date: "20000515122455.50000q000":`+
			` {{hPath0}}:10`+
			` - "20000515122455.50000q000|U|key1|Bad"`,
		`load: invalid timestamp out of sequence:`+
			` received date: 20000515122445.000000000`+
			` last date: 20000515122455.000000000:`+
			` {{hPath0}}:11`+
			` - "20000515122445.000000000|U|key1|Bad"`,
		`splitRecord: invalid date mismatch:`+
			` "19990515":`+
			` {{hPath0}}:12`+
			` - "19990515122455.400000000|U|key1|Bad"`,
		`splitRecord: invalid date mismatch:`+
			` "29990515":`+
			` {{hPath0}}:13`+
			` - "29990515122455.400000000|U|key1|Bad"`,
		`starting path retrieved as: {{hPath0}}`,
		// getHistoryDays parsing errors.
		`splitRecord: invalid number of fields: "3": {{hPath0}}:2`+
			` - "{{clkNano1}}|U|Bad"`,
		`splitRecord: invalid action: "": {{hPath0}}:3`+
			` - "{{clkNano2}}||key1|Bad"`,
		`splitRecord: invalid action: "X": {{hPath0}}:4`+
			` - "{{clkNano3}}|X|key1|Bad"`,
		`splitRecord: invalid action: "UU": {{hPath0}}:5`+
			` - "{{clkNano4}}|UU|key1|Bad"`,
		`splitRecord: invalid key length (>= 2 characters): "": {{hPath0}}:6`+
			` - "{{clkNano5}}|U||Bad"`,
		`splitRecord: invalid key length (>= 2 characters): "k": {{hPath0}}:7`+
			` - "{{clkNano6}}|U|k|Bad"`,
		`splitRecord: invalid date: "200005q5122455.500000000":`+
			` {{hPath0}}:8`+
			` - "200005q5122455.500000000|U|key1|Bad"`,
		`splitRecord: invalid date: "20000515122q55.500000000":`+
			` {{hPath0}}:9`+
			` - "20000515122q55.500000000|U|key1|Bad"`,
		`splitRecord: invalid date: "20000515122455.50000q000":`+
			` {{hPath0}}:10`+
			` - "20000515122455.50000q000|U|key1|Bad"`,
		`addAll: invalid timestamp out of sequence:`+
			` received date: 20000515122445.000000000`+
			` last date: 20000515122455.000000000:`+
			` {{hPath0}}:11`+
			` - "20000515122445.000000000|U|key1|Bad"`,
		`splitRecord: invalid date mismatch:`+
			` "19990515":`+
			` {{hPath0}}:12`+
			` - "19990515122455.400000000|U|key1|Bad"`,
		`splitRecord: invalid date mismatch:`+
			` "29990515":`+
			` {{hPath0}}:13`+
			` - "29990515122455.400000000|U|key1|Bad"`,
	)
}

func TestFile_LoadHistoryInvalidFileClose(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{"", "|U|key1|10"},
			{"", "|U|key1|20"},
		}),
	)

	chk.NoErr(fStore.Open())
	defer closeAndLogIfError(fStore)

	chk.NoErr(fStore.currentFile.Close())

	chk.NoErr(fStore.Close())

	chk.Log(`
		opening file based szStore {{file}} in directory {{dir}}
		starting path retrieved as: {{hPath0}}
		close caused: close {{hPath0}}: file already closed
	`)
}

func TestWStoreBase_LoadHistoryInvalidFile(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, _, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	unknownFile := "UNKNOWN_FILE"
	fPath := filepath.Join(dirName, unknownFile)
	fStore.loadHistory(fPath)

	chk.Log(
		`loadHistory: open ` + fPath +
			`: no such file or directory: ` + fPath + `:0 - ""`,
	)
}

func TestWStoreBase_AddAllInvalidFile(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, _, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	unknownFile := "UNKNOWN_FILE"
	fPath := filepath.Join(dirName, unknownFile)
	fStore.addAll(unknownFile, "", nil)

	chk.Log(
		`addAll(fName="` + unknownFile + `",isWanted=""): open ` + fPath +
			`: no such file or directory`,
	)
}

func TestWStoreBase_GetUnknownInvalidKey(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{"", "|U|key1|Good"},
		}),
	)

	chk.NoErr(fStore.Open())
	defer closeAndLogIfError(fStore)

	ts, v, ok := fStore.get("unknown key")
	chk.False(ok)
	chk.Str(v, "")
	chk.True(ts.IsZero())

	chk.Err(
		fStore.update("k", "", 0),
		"invalid data key",
	)

	chk.Err(
		fStore.update("k|y", "", 0),
		"invalid data key",
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
		`get("unknown key"): unknown data key`,
		`update(key="k",value="") invalid key`,
		`update(key="k|y",value="") invalid key`,
	)
}

func TestWStoreBase_UpdateDeleteOnClosedFile(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	fPath := filepath.Join(dirName, filename) + "_20000515" + fileExtension

	chk.NoErr(fStore.Open())
	chk.NoErr(fStore.currentFile.Close())
	chk.Err(
		fStore.update("datKey", "value", 3),
		"write "+fPath+": file already closed",
	)

	chk.Err(
		fStore.Delete("datKey"),
		"write "+fPath+": file already closed",
	)

	chk.Log(`
    opening file based szStore {{file}} in directory {{dir}}
		starting path generated as: ` + fPath + `
    update(key="datKey",value="value") failed: write ` +
		fPath + `: file already closed
    `,
	)
}

func TestWStoreBase_OpenValidRecordExtraGroupSeperator(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{"", "|U|key1|Good"},
			{"", "|U|key1|Good|extraSeparator"},
		}),
	)

	chk.NoErr(fStore.Open())
	defer closeAndLogIfError(fStore)

	ts, v, ok := fStore.get("key1")
	chk.True(ok)
	chk.Str(ts.Format(fmtTimeStamp), "{{clkNano1}}")
	chk.Str(v, "Good|extraSeparator")

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
	)
}

func TestWStoreBase_OpenHistoryWithAppendToLastFile(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	chk.NoErr(
		buildHistoryFile(chk, 1, dirName, filename, [][2]string{
			{ /* clkNano0  */ "", "|U|key1|YesterdayKey1"},
			{ /* clkNano1  */ "", "|U|key2|YesterdayKey2"},
		}),
	)

	chk.NoErr(
		buildHistoryFile(chk, 0, dirName, filename, [][2]string{
			{ /* clkNano2  */ "", "|U|key1|TodayKey1_1"},
			{ /* clkNano3  */ "", "|U|key2|TodayKey2_1"},
		}),
	)

	chk.NoErr(fStore.Open())
	defer closeAndLogIfError(fStore)

	chk.Int(countFiles(dirName, filename), 2) // Just two files

	chk.NoErr(fStore.update("key1", "TodayKey1_2", 2))
	chk.NoErr(fStore.update("key2", "TodayKey2_2", 4))

	chk.Int(countFiles(dirName, filename), 2) // Last file was appended too.

	validateHistory(chk, fStore, "key1", 2,
		[]string{"{{clkNano0}}", "{{clkNano2}}", "{{clkNano4}}"},
		[]string{"YesterdayKey1", "TodayKey1_1", "TodayKey1_2"},
	)

	validateHistory(chk, fStore, "key2", 2,
		[]string{"{{clkNano1}}", "{{clkNano3}}", "{{clkNano5}}"},
		[]string{"YesterdayKey2", "TodayKey2_1", "TodayKey2_2"},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath0}}`,
	)
}

func TestWStoreBase_OpenHistoryWithNewFileRequired(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	chk.NoErr(
		buildHistoryFile(chk, 2, dirName, filename, [][2]string{
			{ /* clkNano0  */ "", "|U|key1|TwoDaysKey1"},
			{ /* clkNano1  */ "", "|U|key2|TwoDaysKey2"},
		}),
	)

	chk.NoErr(
		buildHistoryFile(chk, 1, dirName, filename, [][2]string{
			{ /* clkNano2  */ "", "|U|key1|YesterdayKey1"},
			{ /* clkNano3  */ "", "|U|key2|YesterdayKey2"},
		}),
	)

	chk.NoErr(fStore.Open())
	defer closeAndLogIfError(fStore)

	chk.Int(countFiles(dirName, filename), 2) // Just two files

	chk.NoErr(fStore.update("key1", "TodayKey1", 2))
	chk.NoErr(fStore.update("key2", "TodayKey2", 4))

	chk.Int(countFiles(dirName, filename), 3) // New file was generated.

	validateHistory(chk, fStore, "key1", 2,
		[]string{"{{clkNano0}}", "{{clkNano2}}", "{{clkNano4}}"},
		[]string{"TwoDaysKey1", "YesterdayKey1", "TodayKey1"},
	)

	validateHistory(chk, fStore, "key2", 2,
		[]string{"{{clkNano1}}", "{{clkNano3}}", "{{clkNano5}}"},
		[]string{"TwoDaysKey2", "YesterdayKey2", "TodayKey2"},
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath1}}`,
	)
}

//nolint:funlen // Ok.
func TestWStoreBase_UseCase1(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	dirName, filename, fStore := setupWStoreBaseWithClock(
		chk,
		time.Date(2000, 5, 15, 12, 24, 56, 0, time.Local),
		time.Second,
	)

	chk.NoErr(
		buildHistoryFile(chk, 2, dirName, filename, [][2]string{
			{ /* clkNano0  */ "", "|U|key1|PreDelete"},
			{ /* clkNano1  */ "", "|U|key2|PreDelete"},
			{ /* clkNano2  */ "", "|D|key1|"},
			{ /* clkNano3  */ "", "|D|key2|"},
			{ /* clkNano4  */ "", "|U|key1|PostDelete1"},
			{ /* clkNano5  */ "", "|U|key2|PostDelete1"},
		}),
	)

	chk.NoErr(
		buildHistoryFile(chk, 1, dirName, filename, [][2]string{
			{ /* clkNano6  */ "", "|U|key1|PostDelete0"},
			{ /* clkNano7  */ "", "|U|key2|PostDelete0"},
		}),
	)

	chk.Err(
		fStore.AddWindow("key1", "win1", time.Second),
		"",
	)

	chk.Err(
		fStore.AddWindow("key2", "win2", time.Second*2),
		"",
	)

	chk.Err(
		fStore.AddWindowThreshold("unknown", "unknown", 1, 2, 3, 4,
			func(_, _ string, _, _ ThresholdReason, _ float64) {
			},
		),
		ErrUnknownDatKey.Error(),
	)

	chk.Err(
		fStore.AddWindowThreshold("key1", "unknown", 1, 3, 6, 8,
			func(_, _ string, _, _ ThresholdReason, _ float64) {
			},
		),
		ErrUnknownWinKey.Error(),
	)

	chk.NoErr(
		fStore.AddWindowThreshold("key1", "win1", 1, 2, 3, 4, func(
			d, k string, f, t ThresholdReason, v float64,
		) {
			log.Printf("Threshold(%q,%q),from: %v, to: %v, value: %g",
				d, k, f, t, v,
			)
		}),
	)

	chk.NoErr(
		fStore.AddWindowThreshold("key2", "win2", 1, 3, 6, 9, func(
			d, k string, f, t ThresholdReason, v float64,
		) {
			log.Printf("Threshold(%q,%q),from: %v, to: %v, value: %g",
				d, k, f, t, v,
			)
		}),
	)

	chk.NoErr(fStore.Open())
	defer closeAndLogIfError(fStore)

	chk.NoErr(fStore.update("key1", "Updated", 2))
	chk.NoErr(fStore.update("key2", "Updated", 4))

	validateHistory(chk, fStore, "key1", 2,
		[]string{"{{clkNano4}}", "{{clkNano6}}", "{{clkNano8}}"},
		[]string{"PostDelete1", "PostDelete0", "Updated"},
	)

	validateHistory(chk, fStore, "key2", 2,
		[]string{"{{clkNano5}}", "{{clkNano7}}", "{{clkNano9}}"},
		[]string{"PostDelete1", "PostDelete0", "Updated"},
	)

	chk.Err(
		fStore.AddWindow("will", "fail", time.Second),
		ErrOpenedWindow.Error(),
	)

	chk.Err(
		fStore.AddWindowThreshold("key1", "win1", 1, 2, 3, 4,
			func(_, _ string, _, _ ThresholdReason, _ float64) {
			},
		),
		ErrOpenedWindowThreshold.Error(),
	)

	count, err := fStore.WindowCount("unknown", "unknown")
	chk.Uint64(count, 0)
	chk.Err(
		err,
		ErrUnknownDatKey.Error(),
	)

	average, err := fStore.WindowAverage("unknown", "unknown")
	chk.Float64(average, 0, 0)
	chk.Err(
		err,
		ErrUnknownDatKey.Error(),
	)

	count, err = fStore.WindowCount("key1", "unknown")
	chk.Uint64(count, 0)
	chk.Err(
		err,
		ErrUnknownWinKey.Error(),
	)

	average, err = fStore.WindowAverage("key1", "unknown")
	chk.Float64(average, 0, 0)
	chk.Err(
		err,
		ErrUnknownWinKey.Error(),
	)

	count, err = fStore.WindowCount("key1", "win1")
	chk.Uint64(count, 1)
	chk.NoErr(err)

	average, err = fStore.WindowAverage("key1", "win1")
	chk.Float64(average, 2, 0)
	chk.NoErr(err)

	chk.NoErr(fStore.Delete("key1"))

	count, err = fStore.WindowCount("key1", "win1")
	chk.Uint64(count, 0)
	chk.Err(
		err,
		ErrNoWinData.Error(),
	)

	average, err = fStore.WindowAverage("key1", "win1")
	chk.Float64(average, 0, 0)
	chk.Err(
		err,
		ErrNoWinData.Error(),
	)

	chk.Log(
		`opening file based szStore {{file}} in directory {{dir}}`,
		`starting path retrieved as: {{hPath1}}`,
		`Threshold("key1","win1"),from: Unknown, to: Low Warning, value: 2`,
		`Threshold("key2","win2"),from: Unknown, to: Normal, value: 4`,
	)
}
