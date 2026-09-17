package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("addr", "localhost:50051", "Target gRPC server address")
	numClients := flag.Int("clients", 1000, "Number of concurrent virtual vehicles")
	pingRate := flag.Int("rate", 10, "Target pings per second per vehicle (10 Hz = 10,000 pings/s @ 1000 clients)")
	duration := flag.Duration("duration", 10*time.Second, "Test duration after ramp-up")
	rampup := flag.Duration("rampup", 2*time.Second, "Ramp-up duration to spawn concurrent vehicles")
	numConns := flag.Int("connections", 20, "Number of underlying multiplexed gRPC connections")
	metricsURL := flag.String("metrics-url", "http://localhost:2112/metrics", "Prometheus metrics scrape URL")
	flag.Parse()

	expectedThroughput := *numClients * *pingRate
	fmt.Println("================================================================================")
	fmt.Println("             GeoPulse High-Concurrency Telemetry Load Tester                    ")
	fmt.Println("================================================================================")
	fmt.Printf(" Target Server        : %s\n", *serverAddr)
	fmt.Printf(" Virtual Vehicles     : %d concurrent streaming agents\n", *numClients)
	fmt.Printf(" Ingestion Frequency  : %d pings/sec/vehicle (interval: %v)\n", *pingRate, time.Second/time.Duration(*pingRate))
	fmt.Printf(" Target Peak Load     : %d pings/sec (egress throughput)\n", expectedThroughput)
	fmt.Printf(" Load Duration        : %v (ramp-up: %v)\n", *duration, *rampup)
	fmt.Printf(" Channel Multiplexing : %d gRPC HTTP/2 connections\n", *numConns)
	fmt.Println("================================================================================")

	// Pre-create shared gRPC connection pool to avoid ephemeral socket exhaustion
	conns := make([]*grpc.ClientConn, *numConns)
	for i := 0; i < *numConns; i++ {
		conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to establish connection #%d: %v", i+1, err)
		}
		defer conn.Close()
		conns[i] = conn
	}

	// Scrape baseline Prometheus metrics if available
	baselineMetrics := scrapeMetrics(*metricsURL)

	var (
		totalSent        atomic.Int64
		totalAcks        atomic.Int64
		totalErrors      atomic.Int64
		streamConnErrors atomic.Int64

		// Histogram of ping send latencies (in microseconds)
		// Sample every Nth ping to avoid memory bloat during massive runs
		sampleRate = 10
		latMu      sync.Mutex
		latencies  = make([]int64, 0, (*numClients*int(duration.Seconds())*(*pingRate))/sampleRate)
	)

	fmt.Printf("\n[1/3] Ramping up %d vehicle connections over %v...\n", *numClients, *rampup)

	startTime := time.Now().Add(*rampup)
	stopTime := startTime.Add(*duration)

	var wg sync.WaitGroup
	rampDelay := *rampup / time.Duration(*numClients)

	testCtx, testCancel := context.WithDeadline(context.Background(), stopTime)
	defer testCancel()

	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		driverID := fmt.Sprintf("sim-fleet-%04d", i+1)
		conn := conns[i%*numConns]

		go func(id string, clientConn *grpc.ClientConn, index int) {
			defer wg.Done()

			// Stagger client connections during ramp-up
			if rampDelay > 0 {
				time.Sleep(time.Duration(index) * rampDelay)
			}

			client := telemetryv1.NewTelemetryServiceClient(clientConn)
			stream, err := client.StreamLocation(testCtx)
			if err != nil {
				streamConnErrors.Add(1)
				return
			}

			// Vehicle initial coordinates around Istanbul metropolitan region
			lat := 41.0082 + (rand.Float64()-0.5)*0.15
			lon := 28.9784 + (rand.Float64()-0.5)*0.20
			bearing := rand.Float64() * 360.0
			speed := 45.0 + rand.Float64()*35.0 // 45 - 80 km/h

			ticker := time.NewTicker(time.Second / time.Duration(*pingRate))
			defer ticker.Stop()

			sentCount := 0

		loop:
			for {
				select {
				case <-testCtx.Done():
					break loop

				case <-ticker.C:
					// Realistic vehicle dead-reckoning kinematics
					dt := 1.0 / float64(*pingRate)
					distMeters := (speed / 3.6) * dt
					radBearing := bearing * math.Pi / 180.0
					radLat := lat * math.Pi / 180.0
					lat += (distMeters * math.Cos(radBearing)) / 111139.0
					lon += (distMeters * math.Sin(radBearing)) / (111139.0 * math.Cos(radLat))
					bearing += (rand.Float64() - 0.5) * 2.0

					ping := &telemetryv1.LocationPing{
						DriverId:  id,
						Latitude:  lat,
						Longitude: lon,
						Bearing:   float32(bearing),
						Speed:     float32(speed),
						Timestamp: time.Now().UnixMilli(),
					}

					sendStart := time.Now()
					if err := stream.Send(ping); err != nil {
						totalErrors.Add(1)
						break loop
					}

					totalSent.Add(1)
					sentCount++

					// Sample send latencies
					if sentCount%sampleRate == 0 {
						elapsedUs := time.Since(sendStart).Microseconds()
						latMu.Lock()
						latencies = append(latencies, elapsedUs)
						latMu.Unlock()
					}
				}
			}

			// Cleanly close send direction and wait for server stream acknowledgment
			ack, err := stream.CloseAndRecv()
			if err != nil {
				// Don't treat context canceled on shutdown as error
				if !strings.Contains(err.Error(), "context") {
					totalErrors.Add(1)
				}
			} else if ack.GetSuccess() {
				totalAcks.Add(1)
			}
		}(driverID, conn, i)
	}

	fmt.Println("[2/3] All clients connected. Streaming telemetry load at peak rate...")

	// Print live progress during load duration
	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()

	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		var lastSent int64 = 0
		lastCheck := time.Now()

		for {
			select {
			case <-testCtx.Done():
				return
			case t := <-progressTicker.C:
				currentSent := totalSent.Load()
				elapsed := t.Sub(lastCheck).Seconds()
				rate := float64(currentSent-lastSent) / elapsed
				lastSent = currentSent
				lastCheck = t

				fmt.Printf("\r>> Elapsed: %5.1fs | Sent Pings: %7d | Ingestion Rate: %6.0f pings/s | Errors: %d",
					time.Since(startTime).Seconds(), currentSent, rate, totalErrors.Load())
			}
		}
	}()

	<-tickerDone
	fmt.Println("\n\n[3/3] Load test duration completed. Waiting for streams to cleanly finalize...")

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Scrape final Prometheus metrics
	finalMetrics := scrapeMetrics(*metricsURL)

	// Compute percentiles
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p90, p95, p99, maxLat float64
	if len(latencies) > 0 {
		p50 = float64(latencies[len(latencies)*50/100]) / 1000.0
		p90 = float64(latencies[len(latencies)*90/100]) / 1000.0
		p95 = float64(latencies[len(latencies)*95/100]) / 1000.0
		p99 = float64(latencies[len(latencies)*99/100]) / 1000.0
		maxLat = float64(latencies[len(latencies)-1]) / 1000.0
	}

	totalPings := totalSent.Load()
	throughput := float64(totalPings) / totalDuration.Seconds()

	fmt.Println("\n================================================================================")
	fmt.Println("             GeoPulse Load Test Benchmark Results (Proof of Work)               ")
	fmt.Println("================================================================================")
	fmt.Printf(" Total Ingested Pings : %d records\n", totalPings)
	fmt.Printf(" Active Test Duration : %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf(" Peak Throughput      : %.2f pings/second (target: %d)\n", throughput, expectedThroughput)
	fmt.Printf(" Completed Stream Acks: %d / %d (%.2f%% success rate)\n", totalAcks.Load(), *numClients, float64(totalAcks.Load())/float64(*numClients)*100)
	fmt.Printf(" Stream Errors        : %d\n", totalErrors.Load())
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" End-to-End Send Latency Percentiles:")
	fmt.Printf("   p50 (Median)       : %6.2f ms\n", p50)
	fmt.Printf("   p90                : %6.2f ms\n", p90)
	fmt.Printf("   p95                : %6.2f ms\n", p95)
	fmt.Printf("   p99                : %6.2f ms\n", p99)
	fmt.Printf("   Max Latency        : %6.2f ms\n", maxLat)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" Prometheus Engine Observability Scraped Values:")
	if finalMetrics != "" {
		fmt.Println(formatMetricsSummary(baselineMetrics, finalMetrics))
	} else {
		fmt.Println("   (Prometheus endpoint not reachable; skipped)")
	}
	fmt.Println("================================================================================")
}

func scrapeMetrics(url string) string {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	var sb strings.Builder
	buf := make([]byte, 8192)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	return sb.String()
}

func formatMetricsSummary(before, after string) string {
	keys := []string{
		"active_connections_count",
		"location_pings_total",
		"batch_flush_records_total",
		"ring_buffer_depth",
		"redis_stale_drivers_pruned_total",
	}

	var sb strings.Builder
	lines := strings.Split(after, "\n")
	for _, k := range keys {
		for _, l := range lines {
			if strings.HasPrefix(l, k) && !strings.HasPrefix(l, "#") {
				sb.WriteString(fmt.Sprintf("   %-36s : %s\n", k, strings.TrimSpace(strings.TrimPrefix(l, k))))
				break
			}
		}
	}
	return sb.String()
}
