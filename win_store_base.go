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
	defaultFilePermissions = 0o0644
	expectedNumberOfFields = 4
	minKeyLength           = 2
	fmtTimeStamp           = "20060102150405.000000000"
	fmtDateStamp           = "20060102"
	fileExtension          = ".dat"
	rangeErrPrefix         = "range: "
	syntaxErrPrefix        = "syntax: "
	base10                 = 10
)

// dataPoint defines an individual storage entry.
type dataPoint struct {
	TS    time.Time
	Value string
}

// fileStore contains data relating to a file storage object.
type fileStore struct {
	rwMutex sync.RWMutex

	opened          bool
	filenameRoot    string
	dirName         string
	currentFile     *os.File
	currentFileDate string
	fileHistory     []string

	// Most recent Values.
	data map[string]*dataPoint

	// Windows.
	winDB map[string]*winDB

	// File record loading.
	fName    string
	fLine    string
	fLineNum uint

	ts func() time.Time
}

// newFileStore opens or creates a fileStore object.
func newFileStore(dirName, filenameRoot string) *fileStore {
	fStore := new(fileStore)
	fStore.opened = false
	fStore.dirName = dirName
	fStore.filenameRoot = filenameRoot
	fStore.data = make(map[string]*dataPoint)
	fStore.winDB = make(map[string]*winDB)
	fStore.ts = time.Now // Default

	return fStore
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
		"opening file based szStore %s in directory %s", fs.filenameRoot, fs.dirName,
	)

	var startingFilePath string
	// Catalog data store file history.

	allFiles, err := os.ReadDir(fs.dirName)
	if err != nil {
		return err //nolint:wrapcheck // Ok.
	}

	for _, fileInf := range allFiles {
		if strings.HasPrefix(fileInf.Name(), fs.filenameRoot) {
			fs.fileHistory = append(fs.fileHistory, fileInf.Name())
		}
	}

	if len(fs.fileHistory) > 0 {
		for _, n := range fs.fileHistory {
			fs.loadHistory(fs.dirName + string(os.PathSeparator) + n)
		}

		startingFilePath = fs.dirName +
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

	return err //nolint:wrapcheck // Ok.
}

func (fs *fileStore) generateFilePath(t time.Time) string {
	return fs.dirName +
		string(os.PathSeparator) +
		fs.filenameRoot + "_" +
		t.Format(fmtDateStamp) + fileExtension
}

func (fs *fileStore) openFile(fPath string) error {
	if fs.currentFile != nil {
		closeAndLogIfError(fs.currentFile)
		fs.currentFileDate = ""
		fs.currentFile = nil
	}

	f, err := os.OpenFile( //nolint:gosec // Ok.
		fPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, defaultFilePermissions,
	)
	if err == nil {
		fs.currentFileDate = fPath[len(fPath)-12 : len(fPath)-4]
		fs.currentFile = f
	}

	return err //nolint:wrapcheck // Ok.
}

func (fs *fileStore) loadHistoryFile(
	fName string, scanner *bufio.Scanner,
) error {
	for scanner.Scan() {
		fs.fLine = scanner.Text()
		fs.fLineNum++

		timestamp, action, datKey, value, ok := fs.splitRecord(fName, fs.fLine)
		if ok {
			if action == ActionDelete {
				wdb, ok := fs.winDB[datKey]
				if ok {
					wdb.delete()
				}

				delete(fs.data, datKey)
			} else {
				fs.load(timestamp, datKey, value)
			}
		}
	}

	return scanner.Err() //nolint:wrapcheck // Ok.
}

func (fs *fileStore) loadHistory(fName string) {
	defer func() {
		fs.fName = ""
		fs.fLineNum = 0
		fs.fLine = ""
	}()

	fs.fName = fName
	fs.fLineNum = 0

	f, err := os.Open(fName) //nolint:gosec // Ok.
	if err == nil {
		err = fs.loadHistoryFile(fName, bufio.NewScanner(f))
	}

	if err != nil {
		fs.logMsg("loadHistory: " + err.Error())
	}
}

//nolint:funlen // Ok.
func (fs *fileStore) splitRecord(filePath string, data string) (
	time.Time,
	Action,
	string,
	string,
	bool,
) {
	const proc = "splitRecord: "

	var (
		err       error
		timestamp time.Time
		action    Action
		key       string
		value     string
	)

	fields := strings.SplitN(data, groupSeparator, expectedNumberOfFields)
	if len(fields) != expectedNumberOfFields {
		return timestamp, action, key, value,
			fs.logMsg(
				proc +
					"invalid number of fields: \"" +
					strconv.FormatInt(int64(len(fields)), 10) + `"`,
			)
	}

	//nolint:gosmopolitan // Internal logs are all in local time.
	timestamp, err = time.ParseInLocation(fmtTimeStamp, fields[0], time.Local)
	if err != nil {
		return timestamp, action, key, value, fs.logMsg(
			proc + "invalid date: \"" + fields[0] + `"`,
		)
	}

	if len(fields[1]) != 1 {
		return timestamp, action, key, value, fs.logMsg(
			proc + "invalid action: \"" + fields[1] + `"`,
		)
	}

	action = Action([]byte(fields[1])[0])
	if action != ActionUpdate && action != ActionDelete {
		return timestamp, action, key, value, fs.logMsg(
			proc + "invalid action: \"" + fields[1] + `"`,
		)
	}

	if !strings.HasPrefix(fields[0], filePath[len(filePath)-12:len(filePath)-4]) {
		return timestamp, action, key, value, fs.logMsg(
			proc + "invalid date mismatch: \"" + fields[0][:8] + `"`,
		)
	}

	if len(fields[2]) < minKeyLength {
		return timestamp, action, key, value, fs.logMsg(
			proc + "invalid key length (>= 2 characters): \"" + fields[2] + `"`,
		)
	}

	key = fields[2]
	value = fields[3]

	return timestamp, action, key, value, true
}

func (fs *fileStore) load(timeStamp time.Time, key, value string) {
	data, ok := fs.data[key]
	if !ok {
		data = new(dataPoint)
		fs.data[key] = data

		if _, ok := fs.winDB[key]; !ok {
			fs.winDB[key] = newWinDB(key)
		}
	} else if data.TS.After(timeStamp) {
		fs.logMsg(
			fmt.Sprintf("load: invalid timestamp out of sequence:"+
				" received date: %s last date: %s",
				timeStamp.Format(fmtTimeStamp),
				data.TS.Format(fmtTimeStamp),
			),
		)

		return
	}

	data.TS = timeStamp
	data.Value = value
}

func (fs *fileStore) writeToFile(
	action Action, key, value string,
) (time.Time, error) {
	var err error

	timestamp := fs.ts()

	if timestamp.Format(fmtDateStamp) != fs.currentFileDate {
		var fileInfo os.FileInfo

		fPath := fs.generateFilePath(timestamp)

		err = fs.openFile(fPath)
		if err == nil {
			fileInfo, err = os.Stat(fPath)
			if err == nil {
				fs.fileHistory = append(fs.fileHistory, fileInfo.Name())
			}
		}
	}

	if err == nil {
		entry := fmt.Sprintf(
			"%s|%c|%s|%s\n",
			timestamp.Format(fmtTimeStamp), action, key, value,
		)
		_, err = fs.currentFile.WriteString(entry)
	}

	return timestamp, err //nolint:wrapcheck // Ok.
}

// get returns the last value set for the specific key.
func (fs *fileStore) get(datKey string) (time.Time, string, bool) {
	fs.rwMutex.RLock()
	defer fs.rwMutex.RUnlock()

	entry, ok := fs.data[datKey]
	if ok {
		return entry.TS, entry.Value, true
	}

	log.Printf("get(%q): %v", datKey, ErrUnknownDatKey)

	return time.Time{}, "", false
}

// getHistoryDays returns all measures since the provided number of days.  A
// zero represents the current day only.
func (fs *fileStore) getHistoryDays(
	datKey string, days uint, add func(Action, time.Time, string),
) {
	fs.rwMutex.RLock()
	defer fs.rwMutex.RUnlock()

	minFile := fs.filenameRoot +
		"_" +
		fs.ts().AddDate(0, 0, -1*int(days)).Format(fmtDateStamp)
	found := false

	for _, filename := range fs.fileHistory {
		if !found {
			if filename >= minFile {
				found = true
			}
		}

		if found {
			fs.addAll(filename, datKey, add)
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

	fPath := fs.dirName + string(os.PathSeparator) + fName
	dataFile, err := os.Open(fPath) //nolint:gosec // Ok.

	if err == nil {
		defer closeAndLogIfError(dataFile)

		fs.fName = fPath
		fs.fLineNum = 0
		scanner := bufio.NewScanner(dataFile)

		for scanner.Scan() {
			fs.fLine = scanner.Text()
			fs.fLineNum++
			timestamp, action, id, value, ok := fs.splitRecord(fPath, scanner.Text())

			if ok && id == idWanted {
				if lastTS.After(timestamp) {
					fs.logMsg(
						fmt.Sprintf("addAll: invalid timestamp out of sequence:"+
							" received date: %s last date: %s",
							timestamp.Format(fmtTimeStamp),
							lastTS.Format(fmtTimeStamp),
						),
					)
				} else {
					add(action, timestamp, value)
					lastTS = timestamp
				}
			}
		}

		err = scanner.Err()
	}

	if err != nil {
		fs.logMsg(
			fmt.Sprintf("addAll(fName=%q,isWanted=%q): %v", fName, idWanted, err),
		)
	}
}

// update adds or changes the value associated with a specific storage key.
func (fs *fileStore) update(
	key string, value string, floatValue float64,
) error {
	fs.rwMutex.Lock()
	defer fs.rwMutex.Unlock()

	var (
		err       error
		timestamp time.Time
	)

	if len(key) < minKeyLength || strings.Contains(key, groupSeparator) {
		log.Printf(
			"update(key=%q,value=%q) invalid key", key, value,
		)

		return ErrInvalidDatKey
	}

	timestamp, err = fs.writeToFile('U', key, value)
	if err != nil {
		fs.logMsg(
			fmt.Sprintf("update(key=%q,value=%q) failed: %v", key, value, err),
		)
	}

	fs.load(timestamp, key, value)
	fs.winDB[key].addValue(timestamp, floatValue)

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
	datKey, winKey string, timePeriod time.Duration,
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

	return winDB.addWindow(winKey, timePeriod)
}

// AddWindowThreshold provides a monitor of a average.
func (fs *fileStore) AddWindowThreshold(datKey, winKey string,
	lowCritical, lowWarning, highWarning, highCritical float64,
	notifyFunc ThresholdNotifyFunc,
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
		notifyFunc,
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
