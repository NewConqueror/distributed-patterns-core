package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	telemetryv1 "geopulse/gen/go/telemetry/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("addr", "localhost:50051", "gRPC server address")
	driverID := flag.String("driver-id", "driver-ist-01", "Driver / vehicle identifier")
	rate := flag.Int("rate", 10, "Pings per second (default: 10)")
	duration := flag.Duration("duration", 0, "Simulation duration (e.g. 10s, 1m). Default 0 = run until interrupted")
	initLat := flag.Float64("lat", 41.0082, "Starting latitude (default: Istanbul 41.0082)")
	initLon := flag.Float64("lon", 28.9784, "Starting longitude (default: Istanbul 28.9784)")
	initSpeed := flag.Float64("speed", 55.0, "Vehicle cruising speed in km/h")
	queryMode := flag.Bool("query", false, "Query nearby drivers instead of streaming telemetry")
	queryRadius := flag.Float64("radius", 5.0, "Search radius in km for query mode")
	queryLimit := flag.Int("limit", 10, "Max drivers to return in query mode")
	flag.Parse()

	// Connect to gRPC server
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server %s: %v", *serverAddr, err)
	}
	defer conn.Close()

	client := telemetryv1.NewTelemetryServiceClient(conn)

	// If query mode is requested, execute GetNearbyDrivers and exit
	if *queryMode {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		req := &telemetryv1.RadiusQuery{
			Latitude:  *initLat,
			Longitude: *initLon,
			RadiusKm:  *queryRadius,
			Limit:     int32(*queryLimit),
		}

		res, err := client.GetNearbyDrivers(ctx, req)
		if err != nil {
			log.Fatalf("Failed to query nearby drivers: %v", err)
		}

		fmt.Println("=======================================================")
		fmt.Println("       GeoPulse Spatial Radius Query Result            ")
		fmt.Println("=======================================================")
		fmt.Printf(" Search Center  : (%.6f, %.6f)\n", *initLat, *initLon)
		fmt.Printf(" Search Radius  : %.2f km\n", *queryRadius)
		fmt.Printf(" Drivers Found  : %d\n", len(res.Drivers))
		fmt.Printf(" Server Time    : %v\n", time.UnixMilli(res.SearchTimestamp).Format(time.RFC3339))
		fmt.Println("------------------------------------------------------------------------------------------------")
		fmt.Printf("%-3s | %-20s | %-9s | %-23s | %-12s | %-8s\n",
			"#", "Driver ID", "Dist (km)", "Coordinates (Lat, Lon)", "Speed (km/h)", "Heading")
		fmt.Println("------------------------------------------------------------------------------------------------")
		for i, d := range res.Drivers {
			fmt.Printf("%-3d | %-20s | %9.3f | (%9.6f, %9.6f) | %12.1f | %7.1f°\n",
				i+1, d.DriverId, d.DistanceKm, d.Latitude, d.Longitude, d.Speed, d.Bearing)
		}
		fmt.Println("================================================================================================")
		return
	}

	if *rate <= 0 {
		*rate = 10
	}
	interval := time.Second / time.Duration(*rate)

	fmt.Println("=======================================================")
	fmt.Println("       GeoPulse Mock Driver Client Simulator          ")
	fmt.Println("=======================================================")
	fmt.Printf(" Target Server  : %s\n", *serverAddr)
	fmt.Printf(" Driver ID      : %s\n", *driverID)
	fmt.Printf(" Ingestion Rate : %d pings/sec (interval: %v)\n", *rate, interval)
	if *duration > 0 {
		fmt.Printf(" Duration       : %v\n", *duration)
	} else {
		fmt.Println(" Duration       : Continuous (Press Ctrl+C to stop)")
	}
	fmt.Println("=======================================================")

	// Context for gRPC stream lifecycle
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	stream, err := client.StreamLocation(streamCtx)
	if err != nil {
		log.Fatalf("Failed to establish LocationStream: %v", err)
	}

	// Capture interrupt signals for graceful stream closing
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	var durationCh <-chan time.Time
	if *duration > 0 {
		timer := time.NewTimer(*duration)
		defer timer.Stop()
		durationCh = timer.C
	}

	lat := *initLat
	lon := *initLon
	bearing := 45.0 // Initial heading northeast (45 degrees)
	speed := *initSpeed

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sentCount := 0
	startTime := time.Now()

	fmt.Println("\nStreaming telemetry pings...")

loop:
	for {
		select {
		case <-sigCh:
			fmt.Println("\n\nStopping simulation (Interrupt received)...")
			break loop
		case <-durationCh:
			fmt.Println("\n\nDuration reached, stopping simulation...")
			break loop
		case <-ticker.C:
			// Advance vehicle along bearing
			// speed in km/h to meters/second: speed / 3.6
			dt := interval.Seconds()
			distMeters := (speed / 3.6) * dt

			// 1 degree latitude ~= 111,139 meters
			// 1 degree longitude ~= 111,139 * cos(lat) meters
			radBearing := bearing * math.Pi / 180.0
			radLat := lat * math.Pi / 180.0

			deltaLat := (distMeters * math.Cos(radBearing)) / 111139.0
			deltaLon := (distMeters * math.Sin(radBearing)) / (111139.0 * math.Cos(radLat))

			lat += deltaLat
			lon += deltaLon

			// Realistic heading wobble (-2.0 to +2.0 degrees)
			bearing += (rand.Float64() - 0.5) * 4.0
			if bearing < 0 {
				bearing += 360
			} else if bearing >= 360 {
				bearing -= 360
			}

			// Slight speed variance (+/- 1.5 km/h)
			speed += (rand.Float64() - 0.5) * 3.0
			if speed < 20 {
				speed = 20
			} else if speed > 90 {
				speed = 90
			}

			ping := &telemetryv1.LocationPing{
				DriverId:  *driverID,
				Latitude:  lat,
				Longitude: lon,
				Bearing:   float32(bearing),
				Speed:     float32(speed),
				Timestamp: time.Now().UnixMilli(),
			}

			if err := stream.Send(ping); err != nil {
				log.Printf("Error sending ping #%d: %v", sentCount+1, err)
				break loop
			}

			sentCount++
			if sentCount%10 == 0 || sentCount == 1 {
				fmt.Printf("\r[Ping #%4d] Driver: %s | Lat: %.6f | Lon: %.6f | Bearing: %5.1f° | Speed: %4.1f km/h",
					sentCount, *driverID, lat, lon, bearing, speed)
			}
		}
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\nClosing client stream (Sent %d pings in %v)...\n", sentCount, elapsed.Round(time.Millisecond))

	// Close send direction and receive server acknowledgment
	ack, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error closing stream and receiving ack: %v", err)
	}

	fmt.Println("\n=======================================================")
	fmt.Println("             Server Stream Acknowledgment             ")
	fmt.Println("=======================================================")
	fmt.Printf(" Success         : %v\n", ack.Success)
	fmt.Printf(" Message         : %s\n", ack.Message)
	fmt.Printf(" Server Processed: %d pings\n", ack.ProcessedCount)
	fmt.Printf(" Server Timestamp: %d (%v)\n", ack.ServerTime, time.UnixMilli(ack.ServerTime).Format(time.RFC3339))
	fmt.Println("=======================================================")
}
