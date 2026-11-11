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

	DefaultTTL = 10 * time.Minute
	MaxTTL     = 60 * time.Minute
)

// ApplyDefaults fills zero-valued fields of s in place.
func ApplyDefaults(s *SandboxSpec) {
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
// a zero cpu_millis or memory_bytes is an error here.
func ValidateSpec(s *SandboxSpec) error {
	cpu := s.GetResources().GetCpuMillis()
	if cpu < MinCPUMillis {
		return fmt.Errorf("cpu_millis %d is below the minimum %d", cpu, MinCPUMillis)
	}
	mem := s.GetResources().GetMemoryBytes()
	if mem < MinMemoryBytes {
		return fmt.Errorf("memory_bytes %d is below the minimum %d (128 MiB)", mem, MinMemoryBytes)
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
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
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
	}
	return nil
}
