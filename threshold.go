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
	"strconv"
	"time"
)

// ThresholdNotifyFunc defines the Threshold callback function.
type ThresholdNotifyFunc func(
	string, // The datKey.
	string, // The winKey.
	ThresholdReason, // Changed from.
	ThresholdReason, // Changed to.
	float64, // The value that caused the threshold change.
)

// ThresholdReason storage type.
type ThresholdReason byte

// Threshold constants.
const (
	ThresholdUnknown      ThresholdReason = 'U'
	ThresholdLowCritical  ThresholdReason = 'c'
	ThresholdLowWarning   ThresholdReason = 'w'
	ThresholdNormal       ThresholdReason = 'N'
	ThresholdHighWarning  ThresholdReason = 'W'
	ThresholdHighCritical ThresholdReason = 'C'
)

func (r ThresholdReason) String() string {
	switch r {
	case ThresholdUnknown:
		return "Unknown"
	case ThresholdLowCritical:
		return "Low Critical"
	case ThresholdLowWarning:
		return "Low Warning"
	case ThresholdNormal:
		return "Normal"
	case ThresholdHighWarning:
		return "High Warning"
	case ThresholdHighCritical:
		return "High Critical"
	default:
		return "InvalidThresholdReason(" +
			strconv.Itoa(int(r)) +
			" - " +
			string(r) +
			")"
	}
}

// Threshold stores and provides methods to act on threshold data.
type threshold struct {
	datKey        string
	winKey        string
	lowCritical   float64
	lowWarning    float64
	highWarning   float64
	highCritical  float64
	currentReason ThresholdReason
	callback      ThresholdNotifyFunc
	started       time.Time
}

// New returns a new Thresholds Data structure.
func newThreshold(
	datKey, winKey string,
	lowCritical, lowWarning, highWarning, highCritical float64,
	notifyFunc ThresholdNotifyFunc,
) (*threshold, error) {
	invalid := false ||
		lowCritical > lowWarning ||
		lowWarning > highWarning ||
		highWarning > highCritical
	if invalid {
		return nil, ErrInvalidThresholdOrder
	}

	if notifyFunc == nil {
		return nil, ErrNilNotifyFunc
	}

	return &threshold{
		datKey:        datKey,
		winKey:        winKey,
		lowCritical:   lowCritical,
		lowWarning:    lowWarning,
		highWarning:   highWarning,
		highCritical:  highCritical,
		callback:      notifyFunc,
		currentReason: ThresholdUnknown,
		started:       time.Now(),
	}, nil
}

// Check determines if the threshold has changed and if so invokes the supplied
// callback function.
func (d *threshold) check(value float64) {
	var newReason ThresholdReason

	switch {
	case value <= d.lowCritical:
		newReason = ThresholdLowCritical
	case value <= d.lowWarning:
		newReason = ThresholdLowWarning
	case value < d.highWarning:
		newReason = ThresholdNormal
	case value < d.highCritical:
		newReason = ThresholdHighWarning
	default:
		newReason = ThresholdHighCritical
	}

	if d.currentReason != newReason {
		oldReason := d.currentReason
		d.currentReason = newReason
		d.callback(d.datKey, d.winKey, oldReason, newReason, value)
	}
}
