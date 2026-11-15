package main

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

// table writes tab-separated rows as aligned columns.
func (c *cli) table(rows [][]string) error {
	tw := tabwriter.NewWriter(c.stdout, 0, 8, 2, ' ', 0)
	for _, r := range rows {
		fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	return tw.Flush()
}

func cpuString(m int64) string {
	if m == 0 {
		return "0"
	}
	return fmt.Sprintf("%dm", m)
}

func memString(b int64) string {
	switch {
	case b == 0:
		return "0"
	case b%(1<<20) == 0:
		return fmt.Sprintf("%dMi", b>>20)
	case b%(1<<10) == 0:
		return fmt.Sprintf("%dKi", b>>10)
	}
	return strconv.FormatInt(b, 10)
}

func (c *cli) query(ctx context.Context, cmd string, args []string, fn func(context.Context, gssv1.SandboxServiceClient) error) error {
	fs := c.flagSet(cmd)
	if err := parse(fs, args); err != nil {
		return err
	}
	if err := noArgs(fs); err != nil {
		return err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	return fn(ctx, sc)
}

func (c *cli) ps(ctx context.Context, args []string) error {
	return c.query(ctx, "ps", args, func(ctx context.Context, sc gssv1.SandboxServiceClient) error {
		resp, err := sc.ListSandboxes(ctx, &gssv1.ListSandboxesRequest{})
		if err != nil {
			return fmt.Errorf("list sandboxes: %s", describe(err))
		}
		sbs := slices.Clone(resp.GetSandboxes())
		sort.SliceStable(sbs, func(i, j int) bool {
			a, b := sbs[i].GetCreatedAt().AsTime(), sbs[j].GetCreatedAt().AsTime()
			if !a.Equal(b) {
				return a.Before(b)
			}
			return sbs[i].GetId() < sbs[j].GetId()
		})
		rows := [][]string{{"ID", "STATE", "NODE", "GROUP", "START", "READY", "CPU", "MEM", "EXPIRES"}}
		for _, sb := range sbs {
			start, ready, expires := "-", "-", "-"
			if sb.GetStartedAs() != gssv1.StartMode_START_MODE_AUTO {
				start = modeName(sb.GetStartedAs())
			}
			if sb.GetStartLatency() != nil {
				ready = msString(sb.GetStartLatency().AsDuration())
			}
			if sb.GetExpiresAt() != nil {
				if left := sb.GetExpiresAt().AsTime().Sub(c.now()).Round(time.Second); left > 0 {
					expires = "in " + left.String()
				} else {
					expires = "expired"
				}
			}
			r := sb.GetSpec().GetResources()
			rows = append(rows, []string{
				sb.GetId(), strings.TrimPrefix(sb.GetState().String(), "SANDBOX_STATE_"), dash(sb.GetNodeId()), dash(sb.GetNodeGroup()),
				start, ready, cpuString(r.GetCpuMillis()), memString(r.GetMemoryBytes()), expires,
			})
		}
		return c.table(rows)
	})
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func (c *cli) nodes(ctx context.Context, args []string) error {
	return c.query(ctx, "nodes", args, func(ctx context.Context, sc gssv1.SandboxServiceClient) error {
		resp, err := sc.ListNodes(ctx, &gssv1.ListNodesRequest{})
		if err != nil {
			return fmt.Errorf("list nodes: %s", describe(err))
		}
		ns := slices.Clone(resp.GetNodes())
		sort.SliceStable(ns, func(i, j int) bool { return ns[i].GetInfo().GetId() < ns[j].GetInfo().GetId() })
		// scripts/cluster.sh finds the NODE, STATE and WARM columns by their headers.
		rows := [][]string{{"NODE", "GROUP", "STATE", "ADDRESS", "CPU", "MEM", "SANDBOXES", "WARM", "HEALTH", "HEARTBEAT"}}
		for _, n := range ns {
			info, alloc := n.GetInfo(), n.GetAllocated()
			hb := "-"
			if n.GetLastHeartbeat() != nil {
				hb = c.now().Sub(n.GetLastHeartbeat().AsTime()).Round(100*time.Millisecond).String() + " ago"
			}
			rows = append(rows, []string{
				info.GetId(), info.GetGroup(), strings.TrimPrefix(n.GetState().String(), "NODE_STATE_"), info.GetAddress(),
				cpuString(alloc.GetCpuMillis()) + "/" + cpuString(info.GetCapacity().GetCpuMillis()),
				memString(alloc.GetMemoryBytes()) + "/" + memString(info.GetCapacity().GetMemoryBytes()),
				fmt.Sprintf("%d/%d", n.GetSandboxes(), info.GetMaxSandboxes()),
				strconv.Itoa(int(n.GetWarmReady())), fmt.Sprintf("%.2f", n.GetHealth()), hb,
			})
		}
		return c.table(rows)
	})
}

func (c *cli) explain(ctx context.Context, args []string) error {
	id, rest, err := sandboxID("explain", args)
	if err != nil {
		return err
	}
	return c.query(ctx, "explain", rest, func(ctx context.Context, sc gssv1.SandboxServiceClient) error {
		sb, err := sc.GetSandbox(ctx, &gssv1.GetSandboxRequest{SandboxId: id})
		if err != nil {
			return fmt.Errorf("get sandbox %s: %s", id, describe(err))
		}
		state := strings.TrimPrefix(sb.GetState().String(), "SANDBOX_STATE_")
		if sb.GetNodeId() != "" {
			fmt.Fprintf(c.stdout, "sandbox %s: %s on node %s (group %s), started as %s, incarnation %d\n",
				sb.GetId(), state, sb.GetNodeId(), sb.GetNodeGroup(), modeName(sb.GetStartedAs()), sb.GetIncarnation())
		} else {
			fmt.Fprintf(c.stdout, "sandbox %s: %s, not on a node\n", sb.GetId(), state)
		}
		if sb.GetMessage() != "" {
			fmt.Fprintf(c.stdout, "message: %s\n", sb.GetMessage())
		}
		p := sb.GetPlacement()
		if p == nil {
			fmt.Fprintln(c.stdout, "no placement recorded")
			return nil
		}
		fmt.Fprintf(c.stdout, "nodes considered: %d, feasible: %d\n", p.GetNodesTotal(), p.GetNodesFeasible())
		if len(p.GetTop()) == 0 {
			fmt.Fprintln(c.stdout, "no candidates")
			return nil
		}
		var plugins []string
		for _, s := range p.GetTop() {
			for name := range s.GetPlugins() {
				if !slices.Contains(plugins, name) {
					plugins = append(plugins, name)
				}
			}
		}
		sort.Strings(plugins)
		fmt.Fprintln(c.stdout, "top candidates (* = the node it runs on):")
		rows := [][]string{append([]string{"", "NODE", "TOTAL"}, plugins...)}
		for _, s := range p.GetTop() {
			mark := ""
			if s.GetNodeId() == sb.GetNodeId() {
				mark = "*"
			}
			row := []string{mark, s.GetNodeId(), fmt.Sprintf("%.3f", s.GetTotal())}
			for _, name := range plugins {
				if v, ok := s.GetPlugins()[name]; ok {
					row = append(row, fmt.Sprintf("%.3f", v))
				} else {
					row = append(row, "-")
				}
			}
			rows = append(rows, row)
		}
		return c.table(rows)
	})
}
