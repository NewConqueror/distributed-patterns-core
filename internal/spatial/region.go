package spatial

import (
	"fmt"
	"math"
)

// RegionResolver maps latitude and longitude coordinates to a discrete regional identifier.
type RegionResolver func(lat, lon float64) string

// DefaultRegionResolver partitions the globe into 0.5-degree grid sectors (~55km).
// For instance, Istanbul (41.0082, 28.9784) yields "region_41.0_28.5".
func DefaultRegionResolver(lat, lon float64) string {
	// Round to 0.5 degree steps
	stepLat := math.Floor(lat*2) / 2
	stepLon := math.Floor(lon*2) / 2
	return fmt.Sprintf("region_%.1f_%.1f", stepLat, stepLon)
}

// FixedRegionResolver returns a fixed region ID for testing or single-region deployments.
func FixedRegionResolver(regionID string) RegionResolver {
	return func(lat, lon float64) string {
		return regionID
	}
}
