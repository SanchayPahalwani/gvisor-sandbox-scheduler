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
		{"memory 191MiB", &SandboxSpec{Resources: res(500, 191<<20)}, "memory_bytes"},
		{"cpu above max", &SandboxSpec{Resources: res(256001, 256<<20)}, "cpu_millis"},
		{"memory above max", &SandboxSpec{Resources: res(500, 1<<40+1)}, "memory_bytes"},
		{"upper boundaries", &SandboxSpec{Resources: res(256000, 1<<40)}, ""},
		{"ttl 61m", &SandboxSpec{Resources: ok, Ttl: durationpb.New(61 * time.Minute)}, "ttl"},
		{"ttl negative", &SandboxSpec{Resources: ok, Ttl: durationpb.New(-time.Second)}, "ttl"},
		{"egress no port", &SandboxSpec{Resources: ok, EgressAllow: []string{"example.com"}}, "egress_allow"},
		{"egress bad port", &SandboxSpec{Resources: ok, EgressAllow: []string{"10.0.0.0/8:99999"}}, "egress_allow"},
		{"egress port zero", &SandboxSpec{Resources: ok, EgressAllow: []string{"example.com:0"}}, "egress_allow"},
		{"egress ipv6", &SandboxSpec{Resources: ok, EgressAllow: []string{"[::1]:443"}}, "IPv6"},
		{"egress space", &SandboxSpec{Resources: ok, EgressAllow: []string{"a b:80"}}, "egress_allow"},
		{"egress newline", &SandboxSpec{Resources: ok, EgressAllow: []string{"a\nb:80"}}, "egress_allow"},
		{"egress wildcard", &SandboxSpec{Resources: ok, EgressAllow: []string{"*:80"}}, "egress_allow"},
		{"egress octet 256", &SandboxSpec{Resources: ok, EgressAllow: []string{"256.1.1.1:80"}}, "egress_allow"},
		{"egress short ip", &SandboxSpec{Resources: ok, EgressAllow: []string{"10.1.2:80"}}, "egress_allow"},
		{"egress hyphen edge", &SandboxSpec{Resources: ok, EgressAllow: []string{"-a.com:80"}}, "egress_allow"},
		{"egress empty label", &SandboxSpec{Resources: ok, EgressAllow: []string{"a..com:80"}}, "egress_allow"},
		{"egress port 65536", &SandboxSpec{Resources: ok, EgressAllow: []string{"a.com:65536"}}, "egress_allow"},
		{"egress port plus", &SandboxSpec{Resources: ok, EgressAllow: []string{"a.com:+443"}}, "egress_allow"},
		{"egress port non-numeric", &SandboxSpec{Resources: ok, EgressAllow: []string{"a.com:x"}}, "egress_allow"},
		{"nil spec", nil, "spec"},
		{"boundaries", &SandboxSpec{Resources: res(100, 192<<20), Ttl: durationpb.New(60 * time.Minute),
			EgressAllow: []string{"a.com:1", "a.com:65535"}}, ""},
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

func TestApplyDefaultsThenValidate(t *testing.T) {
	s := &SandboxSpec{}
	ApplyDefaults(s)
	if err := ValidateSpec(s); err != nil {
		t.Fatalf("defaulted spec invalid: %v", err)
	}
}

func TestApplyDefaultsNil(t *testing.T) {
	ApplyDefaults(nil) // must not panic
}
