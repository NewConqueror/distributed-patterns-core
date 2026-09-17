package proxy

import (
	"net/http"
	"strings"
)

// hopByHopHeaders lists standard RFC 7230 connection-specific headers in lowercase.
var hopByHopHeaders = map[string]struct{}{
	"connection":          {},
	"keep-alive":          {},
	"proxy-authenticate":  {},
	"proxy-authorization": {},
	"te":                  {},
	"trailer":             {},
	"trailers":            {},
	"transfer-encoding":   {},
	"upgrade":             {},
}

// RemoveHopByHopHeaders inspects and strips all hop-by-hop headers from an HTTP header map
// according to RFC 7230, Section 6.1.
// It removes both standard hop-by-hop headers and any custom headers specified in the "Connection" header.
// It safely handles non-canonical map keys.
func RemoveHopByHopHeaders(header http.Header) {
	if header == nil {
		return
	}

	// 1. Identify custom hop-by-hop headers listed in the "Connection" header(s).
	customHopByHop := make(map[string]struct{})
	for k, vals := range header {
		if strings.EqualFold(k, "Connection") {
			for _, connectionVal := range vals {
				for _, token := range strings.Split(connectionVal, ",") {
					trimmed := strings.ToLower(strings.TrimSpace(token))
					if trimmed != "" {
						customHopByHop[trimmed] = struct{}{}
					}
				}
			}
		}
	}

	// 2. Check if TE header allows "trailers" only (RFC 7230 Section 4.3).
	var teVal string
	for k, vals := range header {
		if strings.EqualFold(k, "TE") && len(vals) > 0 {
			teVal = vals[0]
			break
		}
	}
	hasTrailersOnly := strings.EqualFold(strings.TrimSpace(teVal), "trailers")

	// 3. Scan and delete matching headers in a case-insensitive manner.
	for k := range header {
		lowerKey := strings.ToLower(k)
		if _, isStandard := hopByHopHeaders[lowerKey]; isStandard {
			delete(header, k)
			continue
		}
		if _, isCustom := customHopByHop[lowerKey]; isCustom {
			delete(header, k)
			continue
		}
	}

	// 4. Restore "TE: trailers" if originally present.
	if hasTrailersOnly {
		header.Set("TE", "trailers")
	}
}
