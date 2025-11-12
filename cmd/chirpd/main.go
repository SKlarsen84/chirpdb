package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/sklarsen84/chirpdb/pkg/chirp"
)

func main() {
	dir := flag.String("dir", "./data", "data directory")
	addr := flag.String("addr", ":8080", "listen address")
	fsyncEvery := flag.Duration("fsync-every", 5*time.Millisecond, "group fsyncs at this interval (0 = fsync per op)")
	batchMax := flag.Int("batch-max", 256, "max queued ops before a flush")
	mergeEvery := flag.Duration("merge-every", 30*time.Second, "background compaction interval (0 = off)")
	flag.Parse()

	opts := &chirp.Options{
		FsyncInterval:     *fsyncEvery,
		BatchMax:          *batchMax,
		WriterBufferBytes: 1 << 20,
	}
	db, err := chirp.OpenWithOptions(*dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key := cleanKey(r.PathValue("key"))
		body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		defer r.Body.Close()
		if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			var js any
			if err := json.Unmarshal(body, &js); err != nil {
				http.Error(w, "invalid json", 400); return
			}
		}
		if err := db.Put(key, body); err != nil {
			http.Error(w, err.Error(), 500); return
		}
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("GET /kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key := cleanKey(r.PathValue("key"))
		val, err := db.Get(key)
		if err != nil {
			http.Error(w, "not found", 404); return
		}
		ct := "application/octet-stream"
		trim := strings.TrimLeftFunc(string(val), func(r rune) bool { return r <= ' ' })
		if strings.HasPrefix(trim, "{") || strings.HasPrefix(trim, "[") {
			ct = "application/json"
		}
		w.Header().Set("Content-Type", ct)
		w.Write(val)
	})
	mux.HandleFunc("DELETE /kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key := cleanKey(r.PathValue("key"))
		_ = db.Delete(key)
		w.WriteHeader(204)
	})
	mux.HandleFunc("POST /admin/merge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "use POST", 405); return }
		if err := db.Merge(); err != nil {
			http.Error(w, err.Error(), 500); return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"merged":true}`))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })

	srv := &http.Server{ Addr: *addr, Handler: mux }

	// optional background compactor
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *mergeEvery > 0 {
		go func() {
			t := time.NewTicker(*mergeEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					_ = db.Merge()
				}
			}
		}()
	}

	log.Printf("chirpd listening on %s (dir=%s, fsync-every=%s, batch-max=%d)\n", *addr, *dir, opts.FsyncInterval, opts.BatchMax)
	log.Fatal(srv.ListenAndServe())
}

func cleanKey(k string) string {
	k = path.Clean("/" + k)
	return strings.TrimPrefix(k, "/")
}
