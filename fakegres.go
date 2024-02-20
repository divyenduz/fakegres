package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"path"

	_ "github.com/mattn/go-sqlite3"
)

type config struct {
	id       string
	httpPort string
	raftPort string
	pgPort   string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			i++
			continue
		}

		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+2]
			i++
			continue
		}

		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+2]
			i++
			continue
		}

		if arg == "--pg-port" {
			cfg.pgPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	if cfg.pgPort == "" {
		log.Fatal("Missing required parameter: --pg-port")
	}

	return cfg
}

func createTable(db *sql.DB) {
	statement, err := db.Prepare(`CREATE TABLE IF NOT EXISTS kv (
		"key" TEXT PRIMARY KEY,
		"bytes" BLOB		
	  );`)
	if err != nil {
		log.Fatal(err.Error())
	}
	_, err = statement.Exec()
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	cfg := getConfig()

	dataDir := "data"
	err := os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Could not create data directory: %s", err)
	}

	var filename string = path.Join(dataDir, "/data"+cfg.id+".db")

	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err.Error())
	}
	file.Close()
	log.Println("filename created")

	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		log.Fatalf("Could not open sqlite3 db: %s", err)
	}
	defer db.Close()

	createTable(db)

	pe := newPgEngine(db)
	// Start off in clean state
	pe.delete()

	pf := &pgFsm{pe}
	r, err := setupRaft(path.Join(dataDir, "raft"+cfg.id), cfg.id, "localhost:"+cfg.raftPort, pf)
	if err != nil {
		log.Fatal(err)
	}
	hs := httpServer{r}
	http.HandleFunc("/add-follower", hs.addFollowerHandler)
	go func() {
		err := http.ListenAndServe(":"+cfg.httpPort, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
	runPgServer(cfg.pgPort, db, r)
}
