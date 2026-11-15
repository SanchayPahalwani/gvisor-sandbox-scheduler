package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

// flagSet returns the flag set of one command.
func (c *cli) flagSet(cmd string) *flag.FlagSet {
	fs := flag.NewFlagSet("gssctl "+cmd, flag.ContinueOnError)
	fs.SetOutput(c.stderr)
	return fs
}

// parse parses args into fs. Errors the flag package printed are marked so.
func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return &usageError{err: err, printed: true}
	}
	return nil
}

// noArgs fails when the command line has arguments left after the flags.
func noArgs(fs *flag.FlagSet) error {
	if fs.NArg() > 0 {
		return usagef("%s: unexpected argument %q", fs.Name(), fs.Arg(0))
	}
	return nil
}

// sandboxID takes the sandbox ID that must come first in args.
func sandboxID(cmd string, args []string) (string, []string, error) {
	if len(args) == 0 || args[0] == "" || strings.HasPrefix(args[0], "-") {
		return "", nil, usagef("gssctl %s: the sandbox ID comes first: gssctl %s ID", cmd, cmd)
	}
	return args[0], args[1:], nil
}

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// specFlags are the flags that make a SandboxSpec.
type specFlags struct {
	cpu, mem              string
	allow                 stringList
	group, affinity, mode string
	ttl                   time.Duration
}

func addSpecFlags(fs *flag.FlagSet) *specFlags {
	f := &specFlags{}
	fs.StringVar(&f.cpu, "cpu", "500m", "CPU: millicores (500m) or cores (0.5, 2)")
	fs.StringVar(&f.mem, "mem", "256Mi", "memory: a number with Ki, Mi, Gi or Ti, or bytes; at least 192Mi")
	fs.Var(&f.allow, "allow", "allow egress to host:port, ip:port or cidr:port (repeatable; none: no network)")
	fs.StringVar(&f.group, "group", "", "run on a node of this group (default: any)")
	fs.StringVar(&f.affinity, "affinity", "", "prefer the node group where sandboxes with this key run")
	fs.StringVar(&f.mode, "mode", "auto", "start path: auto, cold, restore or warm")
	fs.DurationVar(&f.ttl, "ttl", gssv1.DefaultTTL, "destroy the sandbox after this long")
	return f
}

func (f *specFlags) spec() (*gssv1.SandboxSpec, error) {
	cpu, err := parseCPU(f.cpu)
	if err != nil {
		return nil, usagef("--cpu: %v", err)
	}
	mem, err := parseMem(f.mem)
	if err != nil {
		return nil, usagef("--mem: %v", err)
	}
	mode, err := parseMode(f.mode)
	if err != nil {
		return nil, usagef("--mode: %v", err)
	}
	s := &gssv1.SandboxSpec{
		Resources:   &gssv1.Resources{CpuMillis: cpu, MemoryBytes: mem},
		EgressAllow: f.allow, NodeGroup: f.group, AffinityKey: f.affinity,
		Ttl: durationpb.New(f.ttl), StartMode: mode,
	}
	if err := gssv1.ValidateSpec(s); err != nil {
		return nil, usagef("%v", err)
	}
	return s, nil
}

var (
	millisRE = regexp.MustCompile(`^([0-9]+)m$`)
	coresRE  = regexp.MustCompile(`^([0-9]+)(?:\.([0-9]+))?$`)
	memRE    = regexp.MustCompile(`^([0-9]+)(Ki|Mi|Gi|Ti)?$`)
)

// parseCPU reads millicores ("500m") or cores ("0.5", "2") as millicores.
func parseCPU(s string) (int64, error) {
	var n int64
	if m := millisRE.FindStringSubmatch(s); m != nil {
		v, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%q is out of range", s)
		}
		n = v
	} else if m := coresRE.FindStringSubmatch(s); m != nil {
		whole, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil || whole > math.MaxInt64/1000-1 {
			return 0, fmt.Errorf("%q is out of range", s)
		}
		frac := m[2]
		if len(frac) > 3 {
			return 0, fmt.Errorf("%q is finer than a millicore", s)
		}
		f, _ := strconv.ParseInt(frac+strings.Repeat("0", 3-len(frac)), 10, 64) // three digits
		n = whole*1000 + f
	} else {
		return 0, fmt.Errorf("%q is not millicores (500m) or cores (0.5, 2)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%q must be more than zero", s)
	}
	return n, nil
}

// parseMem reads a byte count with an optional binary suffix: Ki, Mi, Gi or Ti.
func parseMem(s string) (int64, error) {
	m := memRE.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("%q is not a number with Ki, Mi, Gi or Ti, or bytes", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is out of range", s)
	}
	shift := map[string]uint{"": 0, "Ki": 10, "Mi": 20, "Gi": 30, "Ti": 40}[m[2]]
	if n > math.MaxInt64>>shift {
		return 0, fmt.Errorf("%q is out of range", s)
	}
	if n == 0 {
		return 0, fmt.Errorf("%q must be more than zero", s)
	}
	return n << shift, nil
}

func parseMode(s string) (gssv1.StartMode, error) {
	switch s {
	case "auto":
		return gssv1.StartMode_START_MODE_AUTO, nil
	case "cold":
		return gssv1.StartMode_START_MODE_COLD, nil
	case "restore":
		return gssv1.StartMode_START_MODE_RESTORE, nil
	case "warm":
		return gssv1.StartMode_START_MODE_WARM, nil
	}
	return 0, fmt.Errorf("unknown start mode %q (want auto, cold, restore or warm)", s)
}

func modeName(m gssv1.StartMode) string {
	return strings.ToLower(strings.TrimPrefix(m.String(), "START_MODE_"))
}

// cmdFlags are the flags that say what to run.
type cmdFlags struct {
	code, file string
	timeout    time.Duration
}

func addCmdFlags(fs *flag.FlagSet) *cmdFlags {
	f := &cmdFlags{}
	fs.StringVar(&f.code, "c", "", "Python code to run in a fork of the primed interpreter")
	fs.StringVar(&f.file, "f", "", "file of Python code to run the same way")
	fs.DurationVar(&f.timeout, "timeout", defaultExecTimeout, "the program's time limit (at most 10m)")
	return f
}

// request builds the ExecRequest from -c, -f or the ARGV after "--". args is what
// fs parsed.
func (f *cmdFlags) request(fs *flag.FlagSet, args []string) (*gssv1.ExecRequest, error) {
	argv := fs.Args()
	if len(argv) > 0 {
		// The flag package stops at "--" or at the first argument that is not a
		// flag; only the first is a program.
		if n := len(args) - len(argv); n == 0 || args[n-1] != "--" {
			return nil, usagef("%s: unexpected argument %q (a program's arguments go after --)", fs.Name(), argv[0])
		}
	}
	given := 0
	for _, set := range []bool{f.code != "", f.file != "", len(argv) > 0} {
		if set {
			given++
		}
	}
	switch {
	case given == 0:
		return nil, usagef("%s: give the program: -c CODE, -f FILE or -- ARGV...", fs.Name())
	case given > 1:
		return nil, usagef("%s: give only one of -c CODE, -f FILE and -- ARGV...", fs.Name())
	}
	if f.timeout <= 0 {
		return nil, usagef("%s: --timeout must be positive, got %v", fs.Name(), f.timeout)
	}
	req := &gssv1.ExecRequest{Timeout: durationpb.New(f.timeout)}
	switch {
	case f.code != "":
		req.Command = &gssv1.ExecRequest_PythonCode{PythonCode: f.code}
	case f.file != "":
		b, err := os.ReadFile(f.file)
		if err != nil {
			return nil, err
		}
		req.Command = &gssv1.ExecRequest_PythonCode{PythonCode: string(b)}
	default:
		req.Command = &gssv1.ExecRequest_Argv{Argv: &gssv1.Argv{Args: argv}}
	}
	return req, nil
}
