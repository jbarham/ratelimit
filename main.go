package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	threads   = flag.Int("threads", 5, "number of worker threads")
	count     = flag.Int("count", 20, "number of jobs to run")
	limit     = flag.Int("limit", 10, "rate limit jobs/s")
	errorExit = flag.Bool("error", false, "return error for middle job")
)

func main() {
	flag.Parse()
	if *threads < 1 {
		log.Fatal("-threads must be >= 1")
	}
	if *limit < 1 {
		log.Fatal("-limit must be >= 1")
	}

	log.SetFlags(log.Flags() | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)

	limiter := rate.NewLimiter(rate.Limit(*limit), 1)

	wait := func() error {
		return limiter.Wait(context.Background())
	}

	jobs := make(chan int)

	// Start worker threads
	for i := 0; i < *threads; i += 1 {
		n := i
		g.Go(func() error {
			defer func() {
				log.Printf("Worker %d finished", n)
			}()

			if err := wait(); err != nil {
				return err
			}
			for job := range jobs {
				log.Printf("Worker %d got job %d", n, job)
				if *errorExit && job == *count/2 {
					log.Printf("Worker %d exiting with error", n)
					return fmt.Errorf("fake error")
				}
				select {
				case <-gctx.Done():
					return gctx.Err()
				default: // Carry on
				}
				if err := wait(); err != nil {
					return err
				}
			}
			// jobs channel was closed by main thread
			return nil
		})
	}

	// Start main thread to generate jobs
	g.Go(func() error {
		defer close(jobs) // Signals worker threads to finish

		defer func() {
			log.Printf("Main thread finished")
		}()

		n := 0
		for i := 0; ; i += 1 {
			select {
			case jobs <- i:
			case <-gctx.Done():
				return gctx.Err()
			}
			n += 1
			if *count > 0 && n == *count {
				return nil
			}
		}
	})

	// Thread to check for signals to gracefully finish all functions
	go func() {
		defer func() {
			log.Print("Signal thread finished")
		}()

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

		select {
		case sig := <-sigs:
			log.Printf("Received signal: %s", sig)
			cancel() // This triggers a send to the gctx.Done() channel
		case <-gctx.Done():
			// All errgroup threads are finished, so stop listening for signals
			break
		}
	}()

	// Wait for all errgroup goroutines to finish
	start := time.Now()
	err := g.Wait()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Print("Context was canceled")
		} else {
			log.Printf("Received error: %v", err)
		}
	} else {
		runTime := time.Now().Sub(start).Seconds()
		runRate := float64(*count) / runTime
		log.Println("Finished clean")
		log.Printf("Processed %d jobs processed in %.2f/s at rate of %.1f jobs/s (vs rate limit of %d jobs/s)", *count, runTime, runRate, *limit)
	}
}
