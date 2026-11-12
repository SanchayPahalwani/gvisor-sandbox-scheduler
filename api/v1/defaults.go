package gssv1

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

// Defaults and limits for a SandboxSpec.
const (
	DefaultCPUMillis   int64 = 500
	DefaultMemoryBytes int64 = 256 << 20
	MinCPUMillis       int64 = 100
	MinMemoryBytes     int64 = 128 << 20
	MaxCPUMillis       int64 = 256000
	MaxMemoryBytes     int64 = 1 << 40

	DefaultTTL = 10 * time.Minute
	MaxTTL     = 60 * time.Minute
)

// ApplyDefaults fills zero-valued fields of s in place.
func ApplyDefaults(s *SandboxSpec) {
	if s == nil {
		return
	}
	if s.Resources == nil {
		s.Resources = &Resources{}
	}
	if s.Resources.CpuMillis == 0 {
		s.Resources.CpuMillis = DefaultCPUMillis
	}
	if s.Resources.MemoryBytes == 0 {
		s.Resources.MemoryBytes = DefaultMemoryBytes
	}
	if s.Ttl == nil || s.Ttl.AsDuration() == 0 {
		s.Ttl = durationpb.New(DefaultTTL)
	}
}

// ValidateSpec checks s against the limits above. Call ApplyDefaults first;
// a zero cpu_millis or memory_bytes is an error here, while a nil or zero ttl
// is accepted (ApplyDefaults sets it). A nil spec is an error.
func ValidateSpec(s *SandboxSpec) error {
	if s == nil {
		return fmt.Errorf("spec is required")
	}
	cpu := s.GetResources().GetCpuMillis()
	if cpu < MinCPUMillis {
		return fmt.Errorf("cpu_millis %d is below the minimum %d", cpu, MinCPUMillis)
	}
	if cpu > MaxCPUMillis {
		return fmt.Errorf("cpu_millis %d exceeds the maximum %d", cpu, MaxCPUMillis)
	}
	mem := s.GetResources().GetMemoryBytes()
	if mem < MinMemoryBytes {
		return fmt.Errorf("memory_bytes %d is below the minimum %d (128 MiB)", mem, MinMemoryBytes)
	}
	if mem > MaxMemoryBytes {
		return fmt.Errorf("memory_bytes %d exceeds the maximum %d (1 TiB)", mem, MaxMemoryBytes)
	}
	if s.Ttl != nil {
		if err := s.Ttl.CheckValid(); err != nil {
			return fmt.Errorf("ttl is invalid: %w", err)
		}
		ttl := s.Ttl.AsDuration()
		if ttl < 0 {
			return fmt.Errorf("ttl %v is negative", ttl)
		}
		if ttl > MaxTTL {
			return fmt.Errorf("ttl %v exceeds the maximum %v", ttl, MaxTTL)
		}
	}
	for _, e := range s.EgressAllow {
		if err := validateEgress(e); err != nil {
			return fmt.Errorf("egress_allow %q: %w", e, err)
		}
	}
	return nil
}

// validateEgress accepts host:port, ip:port or cidr:port with an IPv4 address.
func validateEgress(e string) error {
	i := strings.LastIndex(e, ":")
	if i < 0 {
		return fmt.Errorf("missing :port")
	}
	host, portStr := e[:i], e[i+1:]
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return fmt.Errorf("port must be 1..65535")
	}
	if host == "" {
		return fmt.Errorf("missing host")
	}
	if strings.ContainsAny(host, ":[]") {
		return fmt.Errorf("IPv6 literals are not supported in v1")
	}
	if strings.Contains(host, "/") {
		p, err := netip.ParsePrefix(host)
		if err != nil || !p.Addr().Is4() {
			return fmt.Errorf("invalid IPv4 CIDR")
		}
		return nil
	}
	if strings.Trim(host, "0123456789.") == "" {
		a, err := netip.ParseAddr(host)
		if err != nil || !a.Is4() {
			return fmt.Errorf("invalid IPv4 address")
		}
		return nil
	}
	return validateHostname(host)
}

// validateHostname checks RFC 1123 labels: [A-Za-z0-9-], 1-63 characters, no
// leading or trailing hyphen, 253 characters in total.
func validateHostname(h string) error {
	if len(h) > 253 {
		return fmt.Errorf("host name is longer than 253 characters")
	}
	for _, l := range strings.Split(h, ".") {
		if len(l) < 1 || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return fmt.Errorf("invalid host name")
		}
		for _, c := range l {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return fmt.Errorf("invalid host name")
			}
		}
	}
	return nil
}
