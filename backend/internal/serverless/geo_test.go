package serverless

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type geoTransport func(*http.Request) (*http.Response, error)

func (f geoTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestServerlessGeoJSUsesExistingProviderAndSharedCache(t *testing.T) {
	cache := cacheFor(t)
	g := NewGeoJSResolver(cache)
	var calls atomic.Int32
	g.client.Transport = geoTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		require.Equal(t, "https://get.geojs.io/v1/ip/geo/8.8.8.8.json", r.URL.String())
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ip":"8.8.8.8","country_code":"US","continent_code":"NA"}`))}, nil
	})
	require.Equal(t, "US", g.Country(context.Background(), "8.8.8.8"))
	require.Equal(t, "NA", g.Location(context.Background(), "8.8.8.8").Continent)
	other := NewGeoJSResolver(cache)
	other.client = g.client
	require.Equal(t, "US", other.Country(context.Background(), "8.8.8.8"))
	require.Equal(t, int32(1), calls.Load())
	require.Empty(t, g.Country(context.Background(), "127.0.0.1"))
	require.Empty(t, g.Country(context.Background(), "10.2.3.4"))
	require.Empty(t, g.Country(context.Background(), "not-ip"))
	require.Equal(t, int32(1), calls.Load())
}
func TestServerlessGeoJSRejectsMismatchedIPAndCachesUnknown(t *testing.T) {
	g := NewGeoJSResolver(cacheFor(t))
	calls := 0
	g.client.Transport = geoTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ip":"1.1.1.1","country_code":"US"}`))}, nil
	})
	require.Empty(t, g.Country(context.Background(), "8.8.4.4"))
	require.Empty(t, g.Country(context.Background(), "8.8.4.4"))
	require.Equal(t, 1, calls)
}

func TestServerlessGeoLocationDoesNotReuseOrOverwriteCountryOnlyCache(t *testing.T) {
	cache := cacheFor(t)
	hash := sha256.Sum256([]byte("8.8.8.8"))
	legacyKey := prefix + "geojs:" + hex.EncodeToString(hash[:])
	require.NoError(t, cache.Set(context.Background(), legacyKey, "US", time.Hour).Err())
	g := NewGeoJSResolver(cache)
	calls := 0
	g.client.Transport = geoTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ip":"8.8.8.8","country_code":"US","continent_code":"NA"}`))}, nil
	})
	require.Equal(t, Location{Country: "US", Continent: "NA"}, g.Location(context.Background(), "8.8.8.8"))
	require.Equal(t, 1, calls)
	require.Equal(t, "US", cache.Get(context.Background(), legacyKey).Val())
}

func TestServerlessGeoLocationValidatesProviderCodes(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		want          Location
	}{
		{"lowercase", `{"ip":"8.8.4.4","country_code":"us","continent_code":"na"}`, Location{Country: "US", Continent: "NA"}},
		{"missing country", `{"ip":"8.8.4.4","continent_code":"NA"}`, Location{}},
		{"invalid continent", `{"ip":"8.8.4.4","country_code":"US","continent_code":"ZZ"}`, Location{Country: "US"}},
		{"missing continent", `{"ip":"8.8.4.4","country_code":"US"}`, Location{Country: "US"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewGeoJSResolver(nil)
			g.client.Transport = geoTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.payload))}, nil
			})
			require.Equal(t, tc.want, g.Location(context.Background(), "8.8.4.4"))
		})
	}
}
