package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ctx = context.Background()
	rdb *redis.Client
)

type Monitor struct {
	Id        int
	Name      string
	URL       string
	Interval  int
	State     string
	LastCheck int64
	Uptime    float64
	Checks    []Check
}

type Check struct {
	Timestamp int64
	Ok        bool
	Status    int
	Latency   float64
	Err       string
}

type Alert struct {
	TimeStamp int64
	Message   string
	Name      string
	Kind      string
}

func monKey(id int) string    { return fmt.Sprintf("monitor:%d", id) }
func checksKey(id int) string { return fmt.Sprintf("Checks:%d", id) }

func saveMonitor(m *Monitor) error {
	b, _ := json.Marshal(m)
	if err := rdb.Set(ctx, monKey(m.Id), b, 0).Err(); err != nil {
		return err
	}
	return rdb.SAdd(ctx, "monitors", m.Id).Err()
}

func loadMonitor(id int) (*Monitor, error) {
	b, err := rdb.Get(ctx, monKey(id)).Bytes()
	if err != nil {
		return nil, err
	}
	m := &Monitor{}
	return m, json.Unmarshal(b, m)
}

func allMonitors() ([]*Monitor, error) {
	ids, err := rdb.SMembers(ctx, "monitors").Result()
	if err != nil {
		return nil, err
	}

	out := make([]*Monitor, 0, len(ids))
	for _, s := range ids {
		id, _ := strconv.Atoi(s)
		m, err := loadMonitor(id)
		if err != nil {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func history(id int, n int) []Check {
	vals, err := rdb.LRange(ctx, checksKey(id), 0, int64(n-1)).Result()
	if err != nil {
		return nil
	}
	cs := make([]Check, 0, len(vals))
	for i := len(vals) - 1; i >= 0; i-- { // oldest first for the graph
		var c Check
		if json.Unmarshal([]byte(vals[i]), &c) == nil {
			cs = append(cs, c)
		}
	}
	return cs
}

func uptime(cs []Check) float64 {
	if len(cs) == 0 {
		return 0
	}
	ok := 0
	for _, c := range cs {
		if c.OK {
			ok++
		}
	}
	return float64(ok) / float64(len(cs)) * 100
}

func main() {
	port := env("PORT", "8080")
	rdb = redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})

	tickSec, _ := strconv.Atoi(env("TICK_SECONDS", "60"))
	seedInterval := env("SEED_INTERVAL", "60")

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	mux := http.NewServerMux()
	mux.HandleFunc("GET/api/monitors", listMonitors)
	mux.HadleFunc("POST/api/monitors", addMonitor)
	mux.HandleFunc("DELETE/api/monitors/{id}", deleteMonitor)
	mux.HandleFunc("GET/api/alerts", listAlerts)
	dummyRoutes(mux)

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	seed(port, seedInterval)
	for i := 0; i < 4; i++ {
		go worker()
	}

	go scheduler(time.Duration(tickSec) * time.Second)

	log.Printf("Listening on :%s (scheduler tick %ds)", port, tickSec)
	if err := http.Serve(ln, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
