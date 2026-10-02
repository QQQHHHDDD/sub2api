package serverless

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// GeoJS is the same provider used by frontend/src/utils/ipGeoLookup.ts.
// Browser localStorage is not trusted routing input. Countries are resolved
// server-side from the validated client IP and cached across ingress replicas.
const geoJSURL = "https://get.geojs.io/v1/ip/geo/"

type CountryResolver interface {
	Country(context.Context, string) string
}
type Location struct {
	Country   string `json:"country"`
	Continent string `json:"continent"`
}

type LocationResolver interface {
	Location(context.Context, string) Location
}
type GeoJSResolver struct {
	cache  *redis.Client
	client *http.Client
	group  singleflight.Group
	slots  chan struct{}
}

func NewGeoJSResolver(cache *redis.Client) *GeoJSResolver {
	return &GeoJSResolver{cache: cache, client: &http.Client{Timeout: 1500 * time.Millisecond,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 8)}
}
func (g *GeoJSResolver) Country(ctx context.Context, raw string) string {
	return g.Location(ctx, raw).Country
}

func (g *GeoJSResolver) Location(ctx context.Context, raw string) Location {
	ip, err := netip.ParseAddr(raw)
	if err != nil {
		return Location{}
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return Location{}
	}
	normalized := ip.String()
	hash := sha256.Sum256([]byte(normalized))
	// Keep the country-only cache untouched for older ingress instances. A
	// separate key ensures an old cached country cannot hide its continent.
	key := prefix + "geojs-location:" + hex.EncodeToString(hash[:])
	if g.cache != nil {
		if value, err := g.cache.Get(ctx, key).Result(); err == nil {
			var location Location
			if json.Unmarshal([]byte(value), &location) == nil && validLocation(location) {
				return location
			}
		}
	}
	// Bound unique misses; same-IP lookups share a single provider call.
	channel := g.group.DoChan(normalized, func() (any, error) {
		select {
		case g.slots <- struct{}{}:
			defer func() { <-g.slots }()
		default:
			return Location{}, nil
		}
		lookupCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		location := Location{}
		request, err := http.NewRequestWithContext(lookupCtx, http.MethodGet, geoJSURL+url.PathEscape(normalized)+".json", nil)
		if err == nil {
			res, e := g.client.Do(request)
			if e == nil {
				body, readErr := io.ReadAll(io.LimitReader(res.Body, 65537))
				_ = res.Body.Close()
				var result struct {
					IP        string `json:"ip"`
					Country   string `json:"country_code"`
					Continent string `json:"continent_code"`
				}
				if res.StatusCode == 200 && readErr == nil && len(body) <= 65536 && json.Unmarshal(body, &result) == nil {
					resolved, e := netip.ParseAddr(result.IP)
					if e == nil && resolved.Unmap() == ip && countryCode.MatchString(strings.ToUpper(result.Country)) {
						location.Country = strings.ToUpper(result.Country)
						if continentCode.MatchString(strings.ToUpper(result.Continent)) {
							location.Continent = strings.ToUpper(result.Continent)
						}
					}
				}
			}
		}
		if g.cache != nil {
			ttl := 24 * time.Hour
			if location.Country == "" || location.Continent == "" {
				ttl = time.Minute
			}
			value, _ := json.Marshal(location)
			cacheCtx, cacheCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cacheCancel()
			_ = g.cache.Set(cacheCtx, key, value, ttl).Err()
		}
		return location, nil
	})
	select {
	case <-ctx.Done():
		return Location{}
	case result := <-channel:
		if location, ok := result.Val.(Location); ok {
			return location
		}
		return Location{}
	}
}

func validLocation(location Location) bool {
	if location.Country == "" {
		return location.Continent == ""
	}
	return countryCode.MatchString(location.Country) && (location.Continent == "" || continentCode.MatchString(location.Continent))
}
