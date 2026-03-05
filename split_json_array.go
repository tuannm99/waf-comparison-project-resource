// split_json_array.go
// Split a huge JSON file whose top-level is an array: [ {...}, {...}, ... ]
// Works even if the whole file is minified into a single line.
// Uses streaming decode + worker pool writers to utilize multiple CPU cores.
//
// Build:
//   go build -o split_json_array split_json_array.go
//
// Run:
//   ./split_json_array --out Legitimate/splits --items 3000 --writers 8 Legitimate/browsing_2024_*.json
//
// Notes:
// - Each output part is a valid JSON array.
// - Tune --items to keep each part < 100MB if needed.
// - Tune --writers and --queue to increase parallelism; higher queue uses more RAM.

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

type job struct {
	outPath string
	items   []json.RawMessage
}

func writePart(j job) error {
	tmp := j.outPath + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	// Buffered write improves throughput a lot
	w := bufio.NewWriterSize(f, 8*1024*1024) // 8MB
	defer func() {
		_ = w.Flush()
	}()

	// Write as JSON array without re-marshalling objects:
	// We keep each item as RawMessage bytes.
	if _, err := w.WriteString("["); err != nil {
		return err
	}
	for i, it := range j.items {
		if i > 0 {
			if _, err := w.WriteString(","); err != nil {
				return err
			}
		}
		if _, err := w.Write(it); err != nil {
			return err
		}
	}
	if _, err := w.WriteString("]"); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, j.outPath)
}

func worker(id int, jobs <-chan job, errs chan<- error, done *uint64, total *uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobs {
		if err := writePart(j); err != nil {
			errs <- fmt.Errorf("writer %d: %w", id, err)
			return
		}
		atomic.AddUint64(done, 1)
	}
}

func splitFile(path, outRoot string, itemsPerPart, writers, queue int) error {
	base := filepath.Base(path)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	outDir := filepath.Join(outRoot, name)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	fmt.Printf("[+] %s\n    out: %s\n    items/part=%d writers=%d queue=%d\n",
		path, outDir, itemsPerPart, writers, queue)

	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()

	// Big buffer helps with large files
	r := bufio.NewReaderSize(in, 16*1024*1024) // 16MB
	dec := json.NewDecoder(r)

	// Validate top-level array
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read first token: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		return fmt.Errorf("expected top-level JSON array '[' but got %v", tok)
	}

	jobs := make(chan job, queue)
	errs := make(chan error, 1)

	var wg sync.WaitGroup
	var doneParts uint64
	var totalParts uint64

	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go worker(i, jobs, errs, &doneParts, &totalParts, &wg)
	}

	// Producer: stream items and batch them
	partIdx := 1
	buf := make([]json.RawMessage, 0, itemsPerPart)

	submit := func(batch []json.RawMessage, idx int) {
		outPath := filepath.Join(outDir, fmt.Sprintf("%s_part%05d.json", name, idx))
		atomic.AddUint64(&totalParts, 1)
		jobs <- job{outPath: outPath, items: batch}
	}

	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			close(jobs)
			wg.Wait()
			return fmt.Errorf("decode item: %w", err)
		}

		// Copy raw bytes because decoder reuses buffers internally sometimes.
		cp := make([]byte, len(raw))
		copy(cp, raw)
		buf = append(buf, json.RawMessage(cp))

		if len(buf) >= itemsPerPart {
			batch := buf
			buf = make([]json.RawMessage, 0, itemsPerPart)
			submit(batch, partIdx)
			partIdx++
		}

		// Early error check without blocking
		select {
		case e := <-errs:
			close(jobs)
			wg.Wait()
			return e
		default:
		}
	}

	// End array token
	tok, err = dec.Token()
	if err != nil {
		close(jobs)
		wg.Wait()
		return fmt.Errorf("read closing token: %w", err)
	}
	delim, ok = tok.(json.Delim)
	if !ok || delim != ']' {
		close(jobs)
		wg.Wait()
		return fmt.Errorf("expected closing ']' but got %v", tok)
	}

	if len(buf) > 0 {
		submit(buf, partIdx)
	}

	close(jobs)
	wg.Wait()

	select {
	case e := <-errs:
		return e
	default:
	}

	fmt.Printf("    done parts: %d\n", atomic.LoadUint64(&doneParts))
	return nil
}

func main() {
	var (
		outRoot      = flag.String("out", "splits", "Output root directory")
		itemsPerPart = flag.Int("items", 3000, "Items per part")
		writers      = flag.Int("writers", max(1, runtime.NumCPU()-1), "Number of writer goroutines")
		queue        = flag.Int("queue", 16, "Max queued write jobs (higher uses more RAM)")
	)
	flag.Parse()
	inputs := flag.Args()
	if len(inputs) == 0 {
		fmt.Fprintln(
			os.Stderr,
			"Usage: split_json_array [--out DIR] [--items N] [--writers N] [--queue N] file1.json [file2.json ...]",
		)
		os.Exit(2)
	}

	for _, p := range inputs {
		fi, err := os.Stat(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Skip %s: %v\n", p, err)
			continue
		}
		if fi.IsDir() {
			fmt.Fprintf(os.Stderr, "Skip %s: is a directory\n", p)
			continue
		}
		if err := splitFile(p, *outRoot, *itemsPerPart, *writers, *queue); err != nil {
			if err == io.EOF {
				fmt.Fprintf(os.Stderr, "Error %s: unexpected EOF\n", p)
			} else {
				fmt.Fprintf(os.Stderr, "Error %s: %v\n", p, err)
			}
			os.Exit(1)
		}
	}
}
