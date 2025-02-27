package main

import (
	"context"
	"database/sql"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	"github.com/oklog/ulid/v2"
)

type Rule struct {
	name   string
	db     *sql.DB
	ctx    context.Context
	ticker *time.Ticker
}

func NewRule(name string, db *sql.DB) *Rule {
	curRandValue := rand.IntN(60)
	if curRandValue < 10 {
		curRandValue += 10
	}
	return &Rule{
		name:   name,
		db:     db,
		ticker: time.NewTicker(time.Second * time.Duration(curRandValue)),
	}
}

func (r *Rule) RunLoop() {
	r.initTable()
	for {
		tm := <-r.ticker.C
		log.Printf("current tick: %s\n", tm.String())
		log.Printf("current table count: %d\n", r.getTableCount())
		r.executeWork()
	}
}

func (r *Rule) initTable() {
	const query = `
		CREATE TABLE IF NOT EXISTS test_table (
			id FixedString(26),
			randValue Int64
		) engine = MergeTree()
		order by id
	`
	_, err := r.db.Exec(query)
	if err != nil {
		panic(err)
	}
}

func (r *Rule) getTableCount() uint64 {
	query := `select count() from test_table`
	res := r.db.QueryRow(query)
	if res.Err() != nil {
		log.Println(res.Err())
		return 0
	}
	vl := uint64(0)
	if err := res.Scan(&vl); err != nil {
		log.Println(err)
		return 0
	}
	return vl

}

func (r *Rule) executeWork() {
	query := `INSERT INTO test_table VALUES (?,?)`
	for i := 0; i < 100; i++ {
		_, err := r.db.Exec(query, ulid.Make(), rand.Int64())
		if err != nil {
			log.Println(err.Error())
			break
		}
	}

	newRand := rand.Int64()
	query = `select * from test_table where randValue > ?`
	rows, err := r.db.Query(query, newRand)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()
	res := make([]interface{}, 0)
	for rows.Next() {
		var s string
		var i int64
		if err := rows.Scan(&s, &i); err != nil {
			log.Println(err)
			continue
		}
		res = append(res, s)
		res = append(res, i)
	}
	log.Printf("collected a total of %d rows from the database...\n", len(res))

}

func createLoop() {
	//fake loop to wait undefinetely
	db, err := sql.Open("chdb", "session=/tmp/chdb_test/;bufferSize=1024;driverType=PARQUET")
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		r := NewRule(ulid.Make().String(), db)
		go r.RunLoop()
	}
	wg.Wait()
}

func main() {
	createLoop()
}
