package service

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"time"

	"github.com/akhilrex/hammond/db"
	"github.com/leekchan/accounting"
)

func FuellyImport(content []byte, userId string) []string {
	stream := bytes.NewReader(content)
	reader := csv.NewReader(stream)
	records, err := reader.ReadAll()

	var errors []string
	if err != nil {
		errors = append(errors, err.Error())
		return errors
	}

	vehicles, err := GetUserVehicles(userId)
	if err != nil {
		errors = append(errors, err.Error())
		return errors
	}
	user, err := GetUserById(userId)

	if err != nil {
		errors = append(errors, err.Error())
		return errors
	}

	var vehicleMap map[string]db.Vehicle = make(map[string]db.Vehicle)
	for _, vehicle := range *vehicles {
		vehicleMap[vehicle.Nickname] = vehicle
	}

	var fillups []db.Fillup
	var expenses []db.Expense
	layout := "2006-01-02 15:04"
	altLayout := "2006-01-02 3:04 PM"

	for index, record := range records {
		if index == 0 {
			continue
		}

		var vehicle db.Vehicle
		var ok bool
		if vehicle, ok = vehicleMap[record[4]]; !ok {
			errors = append(errors, "Found an unmapped vehicle entry at row "+strconv.Itoa(index+1))
		}
		dateStr := record[2] + " " + record[3]
		date, err := time.Parse(layout, dateStr)
		if err != nil {
			date, err = time.Parse(altLayout, dateStr)
		}
		if err != nil {
			errors = append(errors, "Found an invalid date/time at row "+strconv.Itoa(index+1))
		}

		totalCostStr := accounting.UnformatNumber(record[9], 3, user.Currency)
		totalCost64, err := strconv.ParseFloat(totalCostStr, 32)
		if err != nil {
			errors = append(errors, "Found an invalid total cost at row "+strconv.Itoa(index+1))
		}

		totalCost := float32(totalCost64)
		odoStr := accounting.UnformatNumber(record[5], 0, user.Currency)
		odoreading, err := strconv.Atoi(odoStr)
		if err != nil {
			errors = append(errors, "Found an invalid odo reading at row "+strconv.Itoa(index+1))
		}
		location := record[12]

		//Create Fillup
		if record[0] == "Gas" {
			rateStr := accounting.UnformatNumber(record[7], 3, user.Currency)
			ratet64, err := strconv.ParseFloat(rateStr, 32)
			if err != nil {
				errors = append(errors, "Found an invalid cost per gallon at row "+strconv.Itoa(index+1))
			}
			rate := float32(ratet64)

			quantity64, err := strconv.ParseFloat(record[8], 32)
			if err != nil {
				errors = append(errors, "Found an invalid quantity at row "+strconv.Itoa(index+1))
			}
			quantity := float32(quantity64)

			notes := fmt.Sprintf("Octane:%s\nGas Brand:%s\nLocation%s\nTags:%s\nPayment Type:%s\nTire Pressure:%s\nNotes:%s\nMPG:%s",
				record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[1],
			)

			isTankFull := record[6] == "Full"
			fal := false
			fillups = append(fillups, db.Fillup{
				VehicleID:       vehicle.ID,
				FuelUnit:        vehicle.FuelUnit,
				FuelQuantity:    quantity,
				PerUnitPrice:    rate,
				TotalAmount:     totalCost,
				OdoReading:      odoreading,
				IsTankFull:      &isTankFull,
				Comments:        notes,
				FillingStation:  location,
				HasMissedFillup: &fal,
				UserID:          userId,
				Date:            date,
				Currency:        user.Currency,
				DistanceUnit:    user.DistanceUnit,
				Source:          "Fuelly",
			})

		}
		if record[0] == "Service" {
			notes := fmt.Sprintf("Tags:%s\nPayment Type:%s\nNotes:%s",
				record[13], record[14], record[16],
			)
			expenses = append(expenses, db.Expense{
				VehicleID:    vehicle.ID,
				Amount:       totalCost,
				OdoReading:   odoreading,
				Comments:     notes,
				ExpenseType:  record[17],
				UserID:       userId,
				Currency:     user.Currency,
				Date:         date,
				DistanceUnit: user.DistanceUnit,
				Source:       "Fuelly",
			})
		}

	}
	if len(errors) != 0 {
		return errors
	}

	tx := db.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		errors = append(errors, err.Error())
		return errors
	}
	if err := tx.Create(&fillups).Error; err != nil {
		tx.Rollback()
		errors = append(errors, err.Error())
		return errors
	}
	if err := tx.Create(&expenses).Error; err != nil {
		tx.Rollback()
		errors = append(errors, err.Error())
		return errors
	}
	err = tx.Commit().Error
	if err != nil {
		errors = append(errors, err.Error())
	}
	return errors
}

func GasbuddyImport(content []byte, userId string) []string {
	stream := bytes.NewReader(content)
	reader := csv.NewReader(stream)
	records, err := reader.ReadAll()

	var errors []string
	if err != nil {
		errors = append(errors, err.Error())
		return errors
	}

	vehicles, err := GetUserVehicles(userId)
	if err != nil {
		errors = append(errors, err.Error())
		return errors
	}
	user, err := GetUserById(userId)

	if err != nil {
		errors = append(errors, err.Error())
		return errors
	}

	var vehicleMap map[string]db.Vehicle = make(map[string]db.Vehicle)
	for _, vehicle := range *vehicles {
		vehicleMap[vehicle.Nickname] = vehicle
	}

	var fillups []db.Fillup

	// layout YYYY-MM-DD HH:MM:SS
	layout := "2006-01-02 15:04:05"

	for index, record := range records {
		if index == 0 {
			continue
		}

		var vehicle db.Vehicle
		var ok bool
		// vehicle appears in the 13th column
		if vehicle, ok = vehicleMap[record[12]]; !ok {
			errors = append(errors, "Found an unmapped vehicle entry at row "+strconv.Itoa(index+1))
		}
		// date appears in column 1, no alt layout
		dateStr := record[0]
		date, err := time.Parse(layout, dateStr)
		if err != nil {
			errors = append(errors, "Found an invalid date/time at row "+strconv.Itoa(index+1))
		}
		// total cost appears in column 8; Currency can be pulled from column 9
		totalCostStr := record[7]
		totalCost64, err := strconv.ParseFloat(totalCostStr, 32)
		if err != nil {
			errors = append(errors, "Found an invalid total cost at row "+strconv.Itoa(index+1))
		}

		totalCost := float32(totalCost64)
		// odometer reading is in column 15; not sure what user.Currency does here. odometer reading is simply presented as a number
		odoStr := record[14]
		odoreading, err := strconv.Atoi(odoStr)
		if err != nil {
			errors = append(errors, "Found an invalid odo reading at row "+strconv.Itoa(index+1))
		}
		// location could be pulled from a short name (e.g. CITGO) from column 2 or a long name.

		location := record[1] + " at " + record[2] + ", " + record[3] + ", " + record[4] + ", " + record[5]

		//Create Fillup: only fuel records, no service records
		// unit price in column 14; not sure what user.Currency does here.
		rateStr := record[13]
		ratet64, err := strconv.ParseFloat(rateStr, 32)
		if err != nil {
			errors = append(errors, "Found an invalid cost per gallon at row "+strconv.Itoa(index+1))
		}
		rate := float32(ratet64)
		//quantity is in column 11
		quantity64, err := strconv.ParseFloat(record[10], 32)
		if err != nil {
			errors = append(errors, "Found an invalid quantity at row "+strconv.Itoa(index+1))
		}
		quantity := float32(quantity64)
		//pull station link as notes, from column 3
		notes := record[2]

		//fillup in column 18
		isTankFull := record[17] == "Yes"

		fal := false
		//this entry does not include Fuel Type (record[9]), which could map to Hammond's Fuel Subtype. However, this would not capture differences in Diesel (e.g., in NorthEast US, we can get higher grade Diesel sourced from New Brunswick - all listed under "diesel" in gasbuddy).
		fillups = append(fillups, db.Fillup{
			VehicleID:       vehicle.ID,
			FuelUnit:        vehicle.FuelUnit, //this could be pulled from record[11]
			FuelQuantity:    quantity,
			PerUnitPrice:    rate,
			TotalAmount:     totalCost,
			OdoReading:      odoreading,
			IsTankFull:      &isTankFull,
			Comments:        notes,
			FillingStation:  location,
			HasMissedFillup: &fal,
			UserID:          userId,
			Date:            date,
			Currency:        user.Currency,     //this could be pulled from record[8]
			DistanceUnit:    user.DistanceUnit, //odometer units must be mapped correctly between fuely profile and gasbuddy profile
			Source:          "GasBuddy",
		})

	}
	if len(errors) != 0 {
		return errors
	}

	tx := db.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		errors = append(errors, err.Error())
		return errors
	}
	if err := tx.Create(&fillups).Error; err != nil {
		tx.Rollback()
		errors = append(errors, err.Error())
		return errors
	}
	err = tx.Commit().Error
	if err != nil {
		errors = append(errors, err.Error())
	}
	return errors
}
