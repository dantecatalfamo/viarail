package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const ViaDataUrl = "https://tsimobile.viarail.ca/data/allData.json"
const Mdash = "&mdash;"
const DBPath = "viarail.db"
const DBUpdateInterval = 2 * time.Hour
const ListenAddr = "localhost:8085"

//go:embed schema.sql
var schema []byte

type Pull struct {
	ID       uint   `json:"id" db:"id"`
	PulledAt string `json:"pulled_at" db:"pulled_at"`
}

type TimeDiff struct {
	Estimated *string `json:"estimated" db:"estimated"`
	Scheduled *string `json:"scheduled" db:"scheduled"`
}

type StationTime struct {
	TimeDiff
	ID        uint      `json:"id" db:"id"`
	TrainID   uint      `json:"-" db:"train_id"`
	Station   string    `json:"station" db:"station"`
	Code      string    `json:"code" db:"code"`
	ETA       *string   `jsob:"eta" db:"eta"`
	Arrival   *TimeDiff `json:"arrival" db:"arrival"`
	Departure *TimeDiff `json:"departure" db:"departure"`
	Diff      string    `json:"diff" db:"diff"`
	DiffMin   int       `json:"diffMin" db:"diff_min"`
}

type StationTimeScan struct {
	StationTime
	DepartureEstimated *string `db:"departure_estimated"`
	DepartureScheduled *string `db:"departure_scheduled"`
	ArrivalEstimated   *string `db:"arrival_estimated"`
	ArrivalScheduled   *string `db:"arrival_scheduled"`
}

func (s *StationTimeScan) ToStationTime() StationTime {
	stationTime := s.StationTime
	if s.ArrivalScheduled != nil || s.ArrivalEstimated != nil {
		stationTime.Arrival = &TimeDiff{
			Estimated: s.ArrivalEstimated,
			Scheduled: s.ArrivalScheduled,
		}
	}
	if s.DepartureScheduled != nil || s.DepartureEstimated != nil {
		stationTime.Departure = &TimeDiff{
			Estimated: s.DepartureEstimated,
			Scheduled: s.DepartureScheduled,
		}
	}

	return stationTime
}

type Train struct {
	ID        uint          `json:"id" db:"id"`
	PullID    uint          `json:"-" db:"pull_id"`
	Name      string        `json:"name" db:"name"`
	Latitude  *float32      `json:"lat" db:"latitude"`
	Longitude *float32      `json:"lng" db:"longitude"`
	Speed     *float32      `json:"speed" db:"speed"`
	Direction *float32      `json:"direction" db:"direction"`
	Poll      *string       `json:"poll" db:"poll"`
	Departed  bool          `json:"departed" db:"departed"`
	Arrived   bool          `json:"arrived" db:"arrived"`
	From      string        `json:"from" db:"from_station"`
	To        string        `json:"to" db:"to_station"`
	Instance  string        `json:"instance" db:"instance"`
	PollMin   *int          `json:"pollMin" db:"poll_min"`
	Times     []StationTime `json:"times,omitempty"`
}

func main() {
	ctx := context.Background()
	db, err := openDB(ctx, DBPath)
	if err != nil {
		log.Fatalf("opening database: %v", err)
	}

	mux := buildMux(db)

	go updateTask(ctx, db, DBUpdateInterval)

	log.Printf("listening on http://%s", ListenAddr)
	panic(http.ListenAndServe(ListenAddr, mux))
}

func buildMux(db *sqlx.DB) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /api/pulls/", handleGetPulls(db))
	mux.Handle("GET /api/pulls/{pullID}", handleGetTrains(db))
	mux.Handle("GET /api/trains/{trainID}", handleGetTrain(db))

	return mux
}

// updateTask pulls in new train data and stores it in the database.
// It since initially when the program launches, and then once every `DBUpdateInterval`
func updateTask(ctx context.Context, db *sqlx.DB, updateInterval time.Duration) {
	ticker := time.NewTicker(updateInterval)
	for {
		if err := updateTrainData(ctx, db); err != nil {
			log.Printf("update train data: %v", err)
		}
		<-ticker.C
	}
}

func openDB(ctx context.Context, dbPath string) (*sqlx.DB, error) {
	log.Print("opening database")
	db, err := sqlx.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}

	log.Print("executing schema")
	if _, err := db.ExecContext(ctx, string(schema)); err != nil {
		return nil, fmt.Errorf("executing schema: %w", err)
	}

	return db, nil
}

func updateTrainData(ctx context.Context, db *sqlx.DB) error {
	log.Print("fetching train data")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ViaDataUrl, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetching train data: %w", err)
	}
	defer res.Body.Close()

	log.Print("decoding train data")
	data := map[string]Train{}
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return fmt.Errorf("decoding train data: %w", err)
	}

	log.Print("inserting train data")
	if err := insertData(ctx, db, data); err != nil {
		return fmt.Errorf("inserting train data: %w", err)
	}

	return nil
}

func insertData(ctx context.Context, db *sqlx.DB, data map[string]Train) error {
	log.Print("begninning tx")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin train tx: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, "INSERT INTO pulls (pulled_at) VALUES (?)", time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("inserting pull time: %w", err)
	}

	pullID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("getting pull id: %w", err)
	}

	for name, details := range data {
		train, err := tx.ExecContext(ctx, `
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
			if station.Estimated != nil && *station.Estimated == Mdash {
				station.Estimated = nil
			}
			if station.Scheduled != nil && *station.Scheduled == Mdash {
				station.Scheduled = nil
			}
			if station.Departure != nil {
				if station.Departure.Estimated != nil && *station.Departure.Estimated != Mdash {
					departure_estimated = station.Departure.Estimated
				}
				if station.Departure.Scheduled != nil && *station.Departure.Scheduled != Mdash {
					departure_scheduled = station.Departure.Scheduled
				}
			}

			if station.Arrival != nil {
				if station.Arrival.Estimated != nil && *station.Arrival.Estimated != Mdash {
					arrival_estimated = station.Arrival.Estimated
				}
				if station.Arrival.Scheduled != nil && *station.Arrival.Scheduled != Mdash {
					arrival_scheduled = station.Arrival.Scheduled
				}
			}
			if station.ETA != nil && *station.ETA != Mdash {
				eta = station.ETA
			}

			if _, err := tx.ExecContext(ctx, `
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
	return tx.Commit()
}

func getPulls(ctx context.Context, db *sqlx.DB) ([]Pull, error) {
	var pulls []Pull
	if err := db.SelectContext(ctx, &pulls, "SELECT id, pulled_at FROM pulls ORDER BY id"); err != nil {
		return nil, fmt.Errorf("selecting pulls: %w", err)
	}

	return pulls, nil
}

func getTrains(ctx context.Context, db *sqlx.DB, pullID uint, fullData bool) ([]*Train, error) {
	var trains []*Train
	if err := db.SelectContext(ctx, &trains, "SELECT * FROM trains WHERE pull_id = ? ORDER BY name", pullID); err != nil {
		return nil, fmt.Errorf("select trains: %w", err)
	}

	if fullData {
		for idx := range trains {
			stationTimes, err := getStationTimes(ctx, db, trains[idx].ID)
			if err != nil {
				return nil, fmt.Errorf("filling station times for train: %w", err)
			}
			trains[idx].Times = stationTimes
		}
	}

	return trains, nil
}

func getTrain(ctx context.Context, db *sqlx.DB, trainID uint) (*Train, error) {
	train := new(Train)
	if err := sqlx.Get(db, train, "SELECT * FROM trains WHERE id = ?", trainID); err != nil {
		return nil, fmt.Errorf("select train: %w", err)
	}

	stationTimes, err := getStationTimes(ctx, db, trainID)
	if err != nil {
		return nil, fmt.Errorf("filling station times for train: %w", err)
	}

	train.Times = stationTimes

	return train, nil
}

func getStationTimes(ctx context.Context, db *sqlx.DB, trainID uint) ([]StationTime, error) {
	var stationTimes []StationTime
	var stationTimeScans []StationTimeScan
	if err := db.SelectContext(ctx, &stationTimeScans, "SELECT * FROM station_times WHERE train_id = ? ORDER BY id", trainID); err != nil {
		return nil, fmt.Errorf("select station_times: %w", err)
	}

	for _, stationTimeScan := range stationTimeScans {
		stationTimes = append(stationTimes, stationTimeScan.ToStationTime())
	}

	return stationTimes, nil
}

func handleGetPulls(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pulls, err := getPulls(r.Context(), db)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			log.Printf("handling get pulls: %v", err)
			return
		}

		if err := json.NewEncoder(w).Encode(pulls); err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			log.Printf("encoding get pulls: %v", err)
			return
		}
	}
}

func handleGetTrains(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pullID, err := strconv.Atoi(r.PathValue("pullID"))
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			log.Printf("parsing pull id: %v", err)
			return
		}

		var fullData bool
		if r.URL.Query().Get("full") != "" {
			fullData = true
		}

		trains, err := getTrains(r.Context(), db, uint(pullID), fullData)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			log.Printf("handling get trains: %v", err)
			return
		}

		if err := json.NewEncoder(w).Encode(trains); err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			log.Printf("encoding trains: %w", err)
			return
		}
	}
}

func handleGetTrain(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		trainID, err := strconv.Atoi(r.PathValue("trainID"))
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			log.Printf("parsing train id: %v", err)
			return
		}

		train, err := getTrain(r.Context(), db, uint(trainID))
		if err != nil {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			log.Printf("handling get train: %v", err)
			return
		}

		if err := json.NewEncoder(w).Encode(train); err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			log.Printf("encoding trains: %w", err)
			return
		}
	}
}
