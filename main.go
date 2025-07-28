package main

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const ViaDataUrl = "https://tsimobile.viarail.ca/data/allData.json"
const mdash = "&mdash;"

//go:embed schema.sql
var schema []byte

type TimeDiff struct {
	Estimated *string `json:"estimated"`
	Scheduled *string `json:"scheduled"`
}

type StationTime struct {
	TimeDiff
	Station   string    `json:"station"`
	Code      string    `json:"code"`
	ETA       *string   `jsob:"eta"`
	Arrival   *TimeDiff `json:"arrival"`
	Departure *TimeDiff `json:"departure"`
	Diff      string    `json:"diff"`
	DiffMin   int       `json:"diffMin"`
}

type Train struct {
	Latitude  *float32      `json:"lat"`
	Longitude *float32      `json:"lng"`
	Speed     *float32      `json:"speed"`
	Direction *float32      `json:"direction"`
	Poll      *string       `json:"poll"`
	Departed  bool          `json:"departed"`
	Arrived   bool          `json:"arrived"`
	From      string        `json:"from"`
	To        string        `json:"to"`
	Instance  string        `json:"instance"`
	PollMin   *int          `json:"pollMin`
	Times     []StationTime `json:"times"`
}

func main() {
	log.Print("opening database")
	db, err := sql.Open("sqlite3", "viarail.db")
	if err != nil {
		log.Fatalf("opening db: %v", err)
	}

	log.Print("executing schema")
	if _, err := db.Exec(string(schema)); err != nil {
		log.Fatalf("executing schema: %v", err)
	}

	log.Print("fetching data")
	res, err := http.Get(ViaDataUrl)
	if err != nil {
		log.Fatalf("fetching schedule: %v", err)
	}
	defer res.Body.Close()

	log.Print("decoding data")
	data := map[string]Train{}
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		log.Fatalf("decoding respones: %v", err)
	}

	log.Print("inserting data")
	if err := insertData(db, data); err != nil {
		log.Fatalf("inserting data: %v", err)
	}
}

func insertData(db *sql.DB, data map[string]Train) error {
	log.Print("begninning tx")
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin train tx: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.Exec("INSERT INTO pulls (pulled_at) VALUES (?)", time.Now())
	if err != nil {
		return fmt.Errorf("inserting pull time: %w", err)
	}

	pullID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("getting pull id: %w", err)
	}

	for name, details := range data {
		train, err := tx.Exec(`
			INSERT INTO trains (
				pull_id,
				name,
				latitude,
				longitude,
				speed,
				direction,
				poll,
				departed,
				arrived,
				from_station,
				to_station,
				instance,
				poll_min
			) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			pullID,
			name,
			details.Latitude,
			details.Longitude,
			details.Speed,
			details.Direction,
			details.Poll,
			details.Departed,
			details.Arrived,
			details.From,
			details.To,
			details.Instance,
			details.PollMin,
		)
		if err != nil {
			return fmt.Errorf("inserting train: %w", err)
		}

		trainID, err := train.LastInsertId()
		if err != nil {
			return fmt.Errorf("getting train id: %w", err)
		}

		for _, station := range details.Times {
			var departure_estimated, departure_scheduled *string
			var arrival_estimated, arrival_scheduled *string
			var eta *string
			if station.Estimated != nil && *station.Estimated == mdash {
				station.Estimated = nil
			}
			if station.Scheduled != nil && *station.Scheduled == mdash {
				station.Scheduled = nil
			}
			if station.Departure != nil {
				if station.Departure.Estimated != nil && *station.Departure.Estimated != mdash {
					departure_estimated = station.Departure.Estimated
				}
				if station.Departure.Scheduled != nil && *station.Departure.Scheduled != mdash {
					departure_scheduled = station.Departure.Scheduled
				}
			}

			if station.Arrival != nil {
				if station.Arrival.Estimated != nil && *station.Arrival.Estimated != mdash {
					arrival_estimated = station.Arrival.Estimated
				}
				if station.Arrival.Scheduled != nil && *station.Arrival.Scheduled != mdash {
					arrival_scheduled = station.Arrival.Scheduled
				}
			}
			if station.ETA != nil && *station.ETA != mdash {
				eta = station.ETA
			}

			if _, err := tx.Exec(`
				INSERT INTO station_times (
					train_id,
					station,
					code,
					estimated,
					scheduled,
					eta,
					departure_estimated,
					departure_scheduled,
					arrival_estimated,
					arrival_scheduled,
					diff,
					diff_min
				) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
				trainID,
				station.Station,
				station.Code,
				station.Estimated,
				station.Scheduled,
				eta,
				departure_estimated,
				departure_scheduled,
				arrival_estimated,
				arrival_scheduled,
				station.Diff,
				station.DiffMin,
			); err != nil {
				return fmt.Errorf("inserting station_time: %w", err)
			}
		}
	}

	log.Print("committing data")
	tx.Commit()

	return nil
}
