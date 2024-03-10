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

	"github.com/dancsecs/sztest"
)

func testString(chk *sztest.Chk, testValue byte, want string) {
	got := ThresholdReason(testValue).String()
	chk.Strf(got, want, "Reason(\n%b\n)", testValue)
}

func Test_SzWinStore_ThresholdReasonString(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	testString(chk, byte(ThresholdUnknown), "Unknown")
	testString(chk, byte(ThresholdLowCritical), "Low Critical")
	testString(chk, byte(ThresholdLowWarning), "Low Warning")
	testString(chk, byte(ThresholdNormal), "Normal")
	testString(chk, byte(ThresholdHighWarning), "High Warning")
	testString(chk, byte(ThresholdHighCritical), "High Critical")
	testString(chk, 0, "InvalidThresholdReason(0 - "+string(byte(0))+")")
	testString(chk, 1, "InvalidThresholdReason(1 - "+string(byte(1))+")")
	testString(chk, 255, "InvalidThresholdReason(255 - "+string(byte(255))+")")
}

func TestThresholdData_InvalidParameterOrdering(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	th, err := newThreshold("datKey", "winKey", 2, 1, 3, 4, nil)

	chk.Err(err, ErrInvalidThresholdOrder.Error())
	chk.Nil(th)

	_, err = newThreshold("datKey", "winKey", 1, 3, 2, 4, nil)
	chk.Err(err, ErrInvalidThresholdOrder.Error())

	_, err = newThreshold("datKey", "winKey", 1, 2, 4, 3, nil)
	chk.Err(err, ErrInvalidThresholdOrder.Error())
}

func TestThresholdData_InvalidNotifyFunction(t *testing.T) {
	chk := sztest.CaptureNothing(t)
	defer chk.Release()

	th, err := newThreshold("datKey", "winKey", 1, 2, 3, 4, nil)
	if th != nil {
		t.Fatal("unexpected non null threshold returned")
	}

	chk.Err(
		err,
		ErrNilNotifyFunc.Error(),
	)
}

func TestThresholdData_NotifyFunction(t *testing.T) {
	chk := sztest.CaptureLog(t)
	defer chk.Release()

	threshold, err := newThreshold(
		"datKey", "winKey",
		5, 10, 15, 20,
		func(k, w string, o, n ThresholdReason, v float64) {
			log.Printf("Threshold for Key: %s Window: %s changed from: %-20s to: %-20s for value: %f",
				k, w, o, n, v,
			)
		},
	)
	chk.NoErr(err)

	var value float64
	for value = 0; value < 26; value++ {
		threshold.check(value)
	}

	for value = 25; value >= 0; value-- {
		threshold.check(value)
	}

	chk.AddSub(`4\.000000`, "3.000000")
	chk.AddSub(`2\.000000`, "3.000000")
	chk.AddSub(`1\.000000`, "3.000000")
	chk.Log(
		"Threshold for Key: datKey Window: winKey changed from: Unknown              to: Low Critical         for value: 0.000000",
		"Threshold for Key: datKey Window: winKey changed from: Low Critical         to: Low Warning          for value: 6.000000",
		"Threshold for Key: datKey Window: winKey changed from: Low Warning          to: Normal               for value: 11.000000",
		"Threshold for Key: datKey Window: winKey changed from: Normal               to: High Warning         for value: 15.000000",
		"Threshold for Key: datKey Window: winKey changed from: High Warning         to: High Critical        for value: 20.000000",
		"Threshold for Key: datKey Window: winKey changed from: High Critical        to: High Warning         for value: 19.000000",
		"Threshold for Key: datKey Window: winKey changed from: High Warning         to: Normal               for value: 14.000000",
		"Threshold for Key: datKey Window: winKey changed from: Normal               to: Low Warning          for value: 10.000000",
		"Threshold for Key: datKey Window: winKey changed from: Low Warning          to: Low Critical         for value: 5.000000",
	)
}
