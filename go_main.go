package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/jackc/pgx/v4"
)

// Define area code for company phone numbers
const companyAreaCode = 312

// Define the bounds for latitude and longitude of Chicago (where employees live)
const (
	chicagoLatitudeMin  float64 = 41.6445
	chicagoLatitudeMax  float64 = 42.023
	chicagoLongitudeMin float64 = -87.9401
	chicagoLongitudeMax float64 = -87.524
)

func createFakerData(numRows int) [][]interface{} {
	var employeeData [][]interface{}
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numRows; i++ {
		employeeID := gofakeit.Number(100000000, 999999999)
		firstName := gofakeit.FirstName()
		lastName := gofakeit.LastName()
		age := gofakeit.Number(18, 65)
		rating := gofakeit.Float32Range(0, 5)

		email := fmt.Sprintf("%s.%s@company.com", firstName, lastName)
		phoneNumber := fmt.Sprintf("%d-555-%04d", companyAreaCode, rand.Intn(10000))

		jsonContactInfo, _ := json.Marshal(map[string]string{"phone": phoneNumber, "email": email})
		bjsonContactInfo := string(jsonContactInfo)

		latitude, _ := gofakeit.LatitudeInRange(chicagoLatitudeMin, chicagoLatitudeMax)
		longitude, _ := gofakeit.LongitudeInRange(chicagoLongitudeMin, chicagoLongitudeMax)
		address := fmt.Sprintf("POINT(%f %f)", longitude, latitude)

		employeeData = append(employeeData, []interface{}{
			employeeID,
			firstName,
			lastName,
			age,
			rating,
			string(jsonContactInfo),
			bjsonContactInfo,
			address,
		})
	}
	return employeeData
}

func createFullQuery(conn *pgx.Conn, reps, numRows, fakerEntries int) []float64 {
	var fullQueryTimes []float64

	// Prepare the insert statement
	insertQueryFull := `
		INSERT INTO employees 
		(employee_id, first_name, last_name, age, rating, json_contact_info, bjson_contact_info, address)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	for i := 0; i < reps; i++ {
		startTime := time.Now()

		for j := 0; j < numRows; j++ {
			employeeData := createFakerData(fakerEntries)
			sampledData := employeeData[rand.Intn(len(employeeData))]

			_, err := conn.Exec(context.Background(), insertQueryFull,
				sampledData[0], sampledData[1], sampledData[2], sampledData[3],
				sampledData[4], sampledData[5], sampledData[6], sampledData[7])
			if err != nil {
				log.Fatalf("Error inserting data: %v\n", err)
			}
		}

		// Truncate the table
		_, err := conn.Exec(context.Background(), "TRUNCATE TABLE employees")
		if err != nil {
			log.Fatalf("Error truncating table: %v\n", err)
		}

		duration := time.Since(startTime).Seconds()
		fullQueryTimes = append(fullQueryTimes, duration)
	}

	return fullQueryTimes
}

func main() {
	// Database connection configuration
	connConfig := pgx.ConnConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "ExCompany",
		User:     "postgres",
		Password: "<password>",
	}

	// Establish connection
	conn, err := pgx.Connect(context.Background(), connConfig)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close()

	// Define parameters for the function
	reps := 1000
	numRows := 3000
	fakerEntries := 10000

	// Measure time for full query execution
	fullQueryTimes := createFullQuery(conn, reps, numRows, fakerEntries)

	// Print average time
	totalTime := 0.0
	for _, t := range fullQueryTimes {
		totalTime += t
	}
	avgTime := totalTime / float64(reps)
	fmt.Printf("Average full table insertion took %.2f seconds\n", avgTime)
}
