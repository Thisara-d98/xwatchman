package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

const queuekey = "queue:pending"

var pinger = &http.Client{Timeout: 10 * time.Second}

func enqueue(m *Monitor) {
	m.LastCheck = time.Now().Unix()
	if err := saveMonitor(m); err != nil {
		log.Printf("enqueue %d: %v", m.Id, err)
		return
	}
	rdb.LPush(ctx, queuekey, m.ID)
}

func scheduler(tick time.Duration) {
	for t := time.NewTicker(tick); ; <-t.C {
		ms, err := allMonitors()
		if err != nil {
			log.Printf("Scheduler: %v", err)
			continue
		}
		now := time.Now().Unix()
		for _, m := range ms {
			if now-m.LastCheck >= int64(m.Interval) {
				enqueue(m)
			}
		}
	}
}

func worker() {
	for {
		res, err := rdb.BRPop(ctx, 0, queuekey).Result()
		if err != nil {
			log.Printf("Worker: %v", err)
			continue
		}
		id, err := strconv.Atoi(res[1])
		if err != nil {
			continue
		}
		runCheck(id)
	}
}

func runCheck(id int) {
	m, err := loadMonitor(id)
	if err != nil {
		log.Printf("runCheck %d: %v", id, err)
		return
	}

	c := ping(m.URL)
	b, _ := json.Marshal(c)

	rdb.LPush(ctx, checksKey(Id), b)
	rdb.Trim(ctx, checksKey(Id), 0, historyLen-1)

	state := "down"
	if c.Ok {
		state = "up"
	}

	kind := alertKind(m.State, state)
	m.State, m.LastCheck = state, c.Timestamp
	saveMonitor(m)

	if kind != "" {
		raise(m, kind, describe(c))
	}
}

func ping(url string) *Check {
	start := time.Now()
	c := &Check{Timestamp: start.Unix()}
	resp, err := pinger.Get(url)
	c.Latency = time.Since(start).Seconds() * 1000

	if err != nil {
		c.Err = err.Error()
		return c
	}

	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	c.Status = resp.StatusCode
	c.Ok = resp.StatusCode < 400
	return c
}

func alertKind(prev, cur string) string {
	switch {
	case prev == cur:
		return ""
	case cur == "down":
		return "down"
	case prev == "unknown":
		return ""
	default:
		return "up"
	}
	return ""
}

func describe(c Check) string {
	if c.Err != "" {
		return c.Err
	}
	return "HTTP " + strconv.Itoa(c.Status) + " in " + strconv.FormatInt(c.Latency, 10) + "ms"
}

func raise(m *Monitor, kind, msg string) {
	a := Alert{
		TimeStamp: time.Now().Unix(),
		Message:   msg,
		Name:      m.Name,
		Kind:      kind,
	}
	b, _ := json.Marshal(a)

	rdb.LPush(ctx, "alerts", b)
	rdb.LTrim(ctx, "alerts", 0, historyLen-1)
	log.Printf("ALERT %s %q %s - %s", kind, m.Name, m.URL, msg)

	if hook := os.GetEnv("WEBHOOK_URL"); hook != "" {
		resp, err := pinger.Post(hook, "application/json", bytes.NewReader(b))
		if err != nil {
			log.Printf("Webhook error: %v", err)
		}
		resp.Body.Close()
	}
}
