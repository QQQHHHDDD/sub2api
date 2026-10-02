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
type LocationResolver interface {
	CountryResolver
	Continent(context.Context, string) string
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
	country, _ := g.location(ctx, raw)
	return country
}
func (g *GeoJSResolver) Continent(ctx context.Context, raw string) string {
	_, continent := g.location(ctx, raw)
	return continent
}

func (g *GeoJSResolver) location(ctx context.Context, raw string) (string, string) {
	ip, err := netip.ParseAddr(raw)
	if err != nil {
		return "", ""
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return "", ""
	}
	normalized := ip.String()
	hash := sha256.Sum256([]byte(normalized))
	key := prefix + "geojs:" + hex.EncodeToString(hash[:])
	locationKey := prefix + "geojs-location:" + hex.EncodeToString(hash[:])
	if g.cache != nil {
		if value, err := g.cache.Get(ctx, locationKey).Result(); err == nil {
			parts := strings.SplitN(value, "|", 2)
			if len(parts) == 2 {
				country, continent := strings.ToUpper(parts[0]), strings.ToUpper(parts[1])
				if (country == "" || countryCode.MatchString(country)) && (continent == "" || continentCode.MatchString(continent)) {
					return country, continent
				}
			}
		}
		// Keep compatibility with the original country-only cache entries.
		if value, err := g.cache.Get(ctx, key).Result(); err == nil && countryCode.MatchString(value) {
			return value, ""
		}
	}
	// Bound unique misses; same-IP lookups share a single provider call.
	channel := g.group.DoChan(normalized, func() (any, error) {
		select {
		case g.slots <- struct{}{}:
			defer func() { <-g.slots }()
		default:
			return "", nil
		}
		lookupCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		country, continent := "", ""
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
						country = strings.ToUpper(result.Country)
						if continentCode.MatchString(strings.ToUpper(result.Continent)) {
							continent = strings.ToUpper(result.Continent)
						}
					}
				}
			}
		}
		if g.cache != nil {
			ttl := 24 * time.Hour
			value := country + "|" + continent
			if country == "" {
				ttl = time.Minute
				value = "|"
			}
			cacheCtx, cacheCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cacheCancel()
			_ = g.cache.Set(cacheCtx, key, value, ttl).Err()
			_ = g.cache.Set(cacheCtx, locationKey, value, ttl).Err()
		}
		return country + "\x00" + continent, nil
	})
	select {
	case <-ctx.Done():
		return "", ""
	case result := <-channel:
		if value, ok := result.Val.(string); ok {
			parts := strings.SplitN(value, "\x00", 2)
			if len(parts) == 2 {
				return parts[0], parts[1]
			}
		}
		return "", ""
	}
}
