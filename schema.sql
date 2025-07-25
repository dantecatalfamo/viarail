CREATE TABLE IF NOT EXISTS schema_version (
       version INTEGER
);

CREATE TABLE IF NOT EXISTS pulls (
       id INTEGER PRIMARY KEY,
       pulled_at TEXT
);

CREATE TABLE IF NOT EXISTS trains (
       id INTEGER PRIMARY KEY,
       pull_id INTEGER,
       name TEXT,
       latitude REAL,
       longitude REAL,
       speed REAL,
       direction REAL,
       poll TEXT,
       departed BOOL,
       arrived BOOL,
       from_station TEXT,
       to_station TEXT,
       instance TEXT,
       poll_min INTEGER,
       FOREIGN KEY(pull_id) REFERENCES pulls(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS station_times (
       id INTEGER PRIMARY KEY,
       train_id INTEGER,
       station TEXT,
       code TEXT,
       estimated TEXT,
       scheduled TEXT,
       eta TEXT,
       departure_estimated TEXT,
       departure_scheduled TEXT,
       arrival_estimated TEXT,
       arrival_scheduled TEXT,
       diff TEXT,
       diff_min INTEGER,
       FOREIGN KEY(train_id) REFERENCES trains(id) ON DELETE CASCADE
);

-- INSERT INTO schema_version VALUES (1);
