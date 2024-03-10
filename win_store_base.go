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
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	groupSeparator         = "|"
	defaultFilePermissions = 0644
	expectedNumberOfFields = 4
	minKeyLength           = 2
	fmtTimeStamp           = "20060102150405.000000000"
	fmtDateStamp           = "20060102"
)

// dataPoint defines an individual storage entry.
type dataPointx struct {
	Ts    time.Time
	Value string
}

// fileStore contains data relating to a file storage object.
type fileStore struct {
	rwMutex sync.RWMutex

	opened          bool
	fileNameRoot    string
	dir             string
	currentFile     *os.File
	currentFileDate string
	fileHistory     []string

	// Most recent Values.
	data map[string]*dataPointx

	// Windows.
	winDB map[string]*winDB

	// File record loading.
	fName    string
	fLine    string
	fLineNum uint

	ts func() time.Time
}

// newFileStore opens or creates a fileStore object.
func newFileStore(dir, fileNameRoot string) *fileStore {
	fs := new(fileStore)
	fs.opened = false
	fs.dir = dir
	fs.fileNameRoot = fileNameRoot
	fs.data = make(map[string]*dataPointx)
	fs.winDB = make(map[string]*winDB)
	fs.ts = time.Now // Default

	return fs
}

func (fs *fileStore) logMsg(msg string) bool {
	if fs.fName == "" {
		log.Print(msg)
	} else {
		log.Printf(msg+": %s:%d - %q",
			fs.fName, fs.fLineNum, fs.fLine,
		)
	}
	return false
}

// open opens (or creates) a fileStore object.
func (fs *fileStore) Open() error {
	fs.rwMutex.Lock()
	defer fs.rwMutex.Unlock()

	log.Printf(
		"opening file based szStore %s in directory %s", fs.fileNameRoot, fs.dir,
	)

	var startingFilePath string
	// Catalog data store file history.

	allFiles, err := os.ReadDir(fs.dir)
	if err != nil {
		return err
	}

	for _, fileInf := range allFiles {
		if strings.HasPrefix(fileInf.Name(), fs.fileNameRoot) {
			fs.fileHistory = append(fs.fileHistory, fileInf.Name())
		}
	}
	if len(fs.fileHistory) > 0 {
		for _, n := range fs.fileHistory {
			fs.loadHistory(fs.dir + string(os.PathSeparator) + n)
		}
		startingFilePath = fs.dir +
			string(os.PathSeparator) +
			fs.fileHistory[len(fs.fileHistory)-1]
		log.Print("starting path retrieved as: " + startingFilePath)
		err = fs.openFile(startingFilePath)
	} else {
		startingFilePath = fs.generateFilePath(fs.ts())
		log.Print("starting path generated as: " + startingFilePath)
		err = fs.openFile(startingFilePath)
		if err == nil {
			var fi os.FileInfo
			fi, err = os.Stat(startingFilePath)
			if err == nil {
				fs.fileHistory = append(fs.fileHistory, fi.Name())
			}
		}
	}
	if err == nil {
		fs.opened = true
	}
	return err
}

func (fs *fileStore) generateFilePath(t time.Time) string {
	return fs.dir +
		string(os.PathSeparator) +
		fs.fileNameRoot + "_" +
		t.Format(fmtDateStamp) + ".dat"
}

func (fs *fileStore) openFile(fPath string) error {
	if fs.currentFile != nil {
		closeAndLogIfError(fs.currentFile)
		fs.currentFileDate = ""
		fs.currentFile = nil
	}

	f, err := os.OpenFile(
		fPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, defaultFilePermissions,
	)
	if err == nil {
		fs.currentFileDate = fPath[len(fPath)-12 : len(fPath)-4]
		fs.currentFile = f
	}
	return err
}

func (fs *fileStore) loadHistory(fName string) {
	defer func() {
		fs.fName = ""
		fs.fLineNum = 0
		fs.fLine = ""
	}()
	fs.fName = fName
	fs.fLineNum = 0
	f, err := os.Open(fName)
	if err == nil {
		s := bufio.NewScanner(f)
		for s.Scan() {
			fs.fLine = s.Text()
			fs.fLineNum++
			ts, action, datKey, value, ok := fs.splitRecord(fName, fs.fLine)
			if ok {
				if action == ActionDelete {
					wdb, ok := fs.winDB[datKey]
					if ok {
						wdb.delete()
					}
					delete(fs.data, datKey)
				} else {
					fs.load(ts, datKey, value)
				}
			}
		}
		err = s.Err()
	}
	if err != nil {
		fs.logMsg("loadHistory: " + err.Error())
	}
}

func (fs *fileStore) splitRecord(filePath string, data string) (
	ts time.Time,
	action Action,
	key string,
	value string,
	ok bool,
) {
	const proc = "splitRecord: "

	f := strings.SplitN(data, groupSeparator, expectedNumberOfFields)
	if len(f) != expectedNumberOfFields {
		return ts, action, key, value,
			fs.logMsg(
				proc +
					"invalid number of fields: \"" +
					strconv.FormatInt(int64(len(f)), 10) + `"`,
			)
	}
	var err error
	ts, err = time.ParseInLocation(fmtTimeStamp, f[0], time.Local)
	if err != nil {
		return ts, action, key, value, fs.logMsg(
			proc + "invalid date: \"" + f[0] + `"`,
		)
	}

	if len(f[1]) != 1 {
		return ts, action, key, value, fs.logMsg(
			proc + "invalid action: \"" + f[1] + `"`,
		)
	}
	action = Action([]byte(f[1])[0])
	if action != ActionUpdate && action != ActionDelete {
		return ts, action, key, value, fs.logMsg(
			proc + "invalid action: \"" + f[1] + `"`,
		)
	}

	if !strings.HasPrefix(f[0], filePath[len(filePath)-12:len(filePath)-4]) {
		return ts, action, key, value, fs.logMsg(
			proc + "invalid date mismatch: \"" + f[0][:8] + `"`,
		)
	}
	if len(f[2]) < minKeyLength {
		return ts, action, key, value, fs.logMsg(
			proc + "invalid key length (>= 2 characters): \"" + f[2] + `"`,
		)
	}
	key = f[2]
	value = f[3]

	return ts, action, key, value, true
}

func (fs *fileStore) load(timeStamp time.Time, key, value string) {
	t, ok := fs.data[key]
	if !ok {
		t = new(dataPointx)
		fs.data[key] = t
		if _, ok := fs.winDB[key]; !ok {
			fs.winDB[key] = newWinDB(key)
		}
	} else if t.Ts.After(timeStamp) {
		fs.logMsg(
			fmt.Sprintf("load: invalid timestamp out of sequence:"+
				" received date: %s last date: %s",
				timeStamp.Format(fmtTimeStamp),
				t.Ts.Format(fmtTimeStamp),
			),
		)
		return
	}
	t.Ts = timeStamp
	t.Value = value
}

func (fs *fileStore) writeToFile(
	action Action, key, value string,
) (time.Time, error) {
	var err error
	ts := fs.ts()
	if ts.Format(fmtDateStamp) != fs.currentFileDate {
		var fi os.FileInfo
		fPath := fs.generateFilePath(ts)
		err = fs.openFile(fPath)
		if err == nil {
			fi, err = os.Stat(fPath)
			if err == nil {
				fs.fileHistory = append(fs.fileHistory, fi.Name())
			}
		}
	}
	if err == nil {
		entry := fmt.Sprintf(
			"%s|%c|%s|%s\n",
			ts.Format(fmtTimeStamp), action, key, value,
		)
		_, err = fs.currentFile.WriteString(entry)
	}
	return ts, err
}

// get returns the last value set for the specific key.
func (fs *fileStore) get(datKey string) (time.Time, string, bool) {
	fs.rwMutex.RLock()
	defer fs.rwMutex.RUnlock()

	entry, ok := fs.data[datKey]
	if ok {
		return entry.Ts, entry.Value, true
	}
	log.Printf("get(%q): %v", datKey, ErrUnknownDatKey)
	return time.Time{}, "", false
}

// getHistoryDays returns all measures since the provided number of days.  A
// zero represents the current day only.
func (fs *fileStore) getHistoryDays(
	id string, days uint, add func(Action, time.Time, string),
) {
	fs.rwMutex.RLock()
	defer fs.rwMutex.RUnlock()

	minFile := fs.fileNameRoot +
		"_" +
		fs.ts().AddDate(0, 0, -1*int(days)).Format(fmtDateStamp)
	found := false
	for _, fi := range fs.fileHistory {
		if !found {
			if fi >= minFile {
				found = true
			}
		}
		if found {
			fs.addAll(fi, id, add)
		}
	}
}

func (fs *fileStore) addAll(
	fName, idWanted string, add func(Action, time.Time, string),
) {
	var lastTS time.Time
	defer func() {
		fs.fName = ""
		fs.fLineNum = 0
		fs.fLine = ""
	}()

	fPath := fs.dir + string(os.PathSeparator) + fName
	f, err := os.Open(fPath)
	if err == nil {
		defer closeAndLogIfError(f)
		fs.fName = fPath
		fs.fLineNum = 0
		s := bufio.NewScanner(f)
		for s.Scan() {
			fs.fLine = s.Text()
			fs.fLineNum++
			ts, action, id, value, ok := fs.splitRecord(fPath, s.Text())
			if ok && id == idWanted {
				if lastTS.After(ts) {
					fs.logMsg(
						fmt.Sprintf("addAll: invalid timestamp out of sequence:"+
							" received date: %s last date: %s",
							ts.Format(fmtTimeStamp),
							lastTS.Format(fmtTimeStamp),
						),
					)
				} else {
					add(action, ts, value)
					lastTS = ts
				}
			}
		}
		err = s.Err()
	}
	if err != nil {
		fs.logMsg(
			fmt.Sprintf("addAll(fName=%q,isWanted=%q): %v", fName, idWanted, err),
		)
	}
}

// update adds or changes the value associated with a specific storage key.
func (fs *fileStore) update(
	key string, value string, f float64,
) error {
	fs.rwMutex.Lock()
	defer fs.rwMutex.Unlock()

	var err error
	var ts time.Time
	if len(key) < minKeyLength || strings.Contains(key, groupSeparator) {
		log.Printf(
			"update(key=%q,value=%q) invalid key", key, value,
		)
		return ErrInvalidDatKey
	}

	ts, err = fs.writeToFile('U', key, value)
	if err != nil {
		fs.logMsg(
			fmt.Sprintf("update(key=%q,value=%q) failed: %v", key, value, err),
		)
	}
	fs.load(ts, key, value)
	fs.winDB[key].addValue(ts, f)
	return err
}

// Delete removes a specific key from the Store.
func (fs *fileStore) Delete(datKey string) error {
	fs.rwMutex.Lock()
	defer fs.rwMutex.Unlock()

	wdb, ok := fs.winDB[datKey]
	if ok {
		wdb.delete()
	}
	delete(fs.data, datKey)
	_, err := fs.writeToFile('D', datKey, "")
	return err
}

// Close the file when program exits.
func (fs *fileStore) Close() error {
	fs.rwMutex.Lock()
	defer fs.rwMutex.Unlock()

	fileToClose := fs.currentFile
	fs.currentFileDate = ""
	fs.currentFile = nil
	fs.opened = false
	if fileToClose != nil {
		closeAndLogIfError(fileToClose)
	}
	return nil
}

// AddWindow creates a named window for the specified key.
func (fs *fileStore) AddWindow(
	datKey, winKey string, p time.Duration,
) error {
	fs.rwMutex.Lock()
	defer fs.rwMutex.Unlock()

	if fs.opened {
		return ErrOpenedWindow
	}

	winDB, ok := fs.winDB[datKey]
	if !ok {
		winDB = newWinDB(datKey)
		fs.winDB[datKey] = winDB
	}
	return winDB.addWindow(winKey, p)
}

// AddWindowThreshold provides a monitor of a average.
func (fs *fileStore) AddWindowThreshold(datKey, winKey string,
	lowCritical, lowWarning, highWarning, highCritical float64,
	f ThresholdCallbackFunc,
) error {
	fs.rwMutex.Lock()
	defer fs.rwMutex.Unlock()

	if fs.opened {
		return ErrOpenedWindowThreshold
	}

	dw, ok := fs.winDB[datKey]
	if !ok {
		return ErrUnknownDatKey
	}
	return dw.addThreshold(winKey,
		lowCritical, lowWarning, highWarning, highCritical,
		f,
	)
}

// WindowAverage returns the specified window average.
func (fs *fileStore) WindowAverage(datKey, winKey string) (float64, error) {
	fs.rwMutex.RLock()
	defer fs.rwMutex.RUnlock()

	dw, ok := fs.winDB[datKey]
	if !ok {
		return 0, ErrUnknownDatKey
	}
	return dw.getAvg(winKey)
}

// WindowCount returns the specified window sample count.
func (fs *fileStore) WindowCount(datKey, winKey string) (uint64, error) {
	fs.rwMutex.RLock()
	defer fs.rwMutex.RUnlock()

	dw, ok := fs.winDB[datKey]
	if !ok {
		return 0, ErrUnknownDatKey
	}
	return dw.getCount(winKey)
}
