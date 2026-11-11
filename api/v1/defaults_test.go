package gssv1

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

func TestApplyDefaults(t *testing.T) {
	s := &SandboxSpec{}
	ApplyDefaults(s)
	if s.GetResources().GetCpuMillis() != 500 {
		t.Errorf("cpu_millis = %d, want 500", s.GetResources().GetCpuMillis())
	}
	if s.GetResources().GetMemoryBytes() != 256<<20 {
		t.Errorf("memory_bytes = %d, want %d", s.GetResources().GetMemoryBytes(), 256<<20)
	}
	if s.GetTtl().AsDuration() != 10*time.Minute {
		t.Errorf("ttl = %v, want 10m", s.GetTtl().AsDuration())
	}
	if s.GetStartMode() != StartMode_START_MODE_AUTO {
		t.Errorf("start_mode = %v, want AUTO", s.GetStartMode())
	}
}

func TestApplyDefaultsKeepsSetValues(t *testing.T) {
	s := &SandboxSpec{
		Resources: &Resources{CpuMillis: 1000, MemoryBytes: 512 << 20},
		Ttl:       durationpb.New(5 * time.Minute),
	}
	ApplyDefaults(s)
	if s.Resources.CpuMillis != 1000 || s.Resources.MemoryBytes != 512<<20 || s.Ttl.AsDuration() != 5*time.Minute {
		t.Errorf("set values changed: %v", s)
	}
}

func TestValidateSpec(t *testing.T) {
	res := func(cpu, mem int64) *Resources { return &Resources{CpuMillis: cpu, MemoryBytes: mem} }
	ok := res(500, 256<<20)
	tests := []struct {
		name    string
		spec    *SandboxSpec
		wantErr string
	}{
		{"cpu zero", &SandboxSpec{Resources: res(0, 256<<20)}, "cpu_millis"},
		{"cpu negative", &SandboxSpec{Resources: res(-1, 256<<20)}, "cpu_millis"},
		{"cpu 99", &SandboxSpec{Resources: res(99, 256<<20)}, "cpu_millis"},
		{"memory 127MiB", &SandboxSpec{Resources: res(500, 127<<20)}, "memory_bytes"},
		{"ttl 61m", &SandboxSpec{Resources: ok, Ttl: durationpb.New(61 * time.Minute)}, "ttl"},
		{"ttl negative", &SandboxSpec{Resources: ok, Ttl: durationpb.New(-time.Second)}, "ttl"},
		{"egress no port", &SandboxSpec{Resources: ok, EgressAllow: []string{"example.com"}}, "egress_allow"},
		{"egress bad port", &SandboxSpec{Resources: ok, EgressAllow: []string{"10.0.0.0/8:99999"}}, "egress_allow"},
		{"egress port zero", &SandboxSpec{Resources: ok, EgressAllow: []string{"example.com:0"}}, "egress_allow"},
		{"egress ipv6", &SandboxSpec{Resources: ok, EgressAllow: []string{"[::1]:443"}}, "egress_allow"},
		{"egress empty host", &SandboxSpec{Resources: ok, EgressAllow: []string{":443"}}, "egress_allow"},
		{"egress bad cidr", &SandboxSpec{Resources: ok, EgressAllow: []string{"10.0.0.0/33:443"}}, "egress_allow"},
		{"valid", &SandboxSpec{Resources: ok, Ttl: durationpb.New(time.Minute),
			EgressAllow: []string{"example.com:443", "10.1.2.3:8080", "10.0.0.0/8:443"}}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSpec(tc.spec)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}
