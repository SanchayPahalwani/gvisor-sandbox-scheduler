package telemetry

import (
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"testing"
)

// dashboardPath is deploy/grafana/dashboard.json, from this package's directory.
const dashboardPath = "../../deploy/grafana/dashboard.json"

type datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type dashboard struct {
	Title  string `json:"title"`
	UID    string `json:"uid"`
	Panels []struct {
		Title      string     `json:"title"`
		Type       string     `json:"type"`
		Datasource datasource `json:"datasource"`
		Targets    []struct {
			Expr       string     `json:"expr"`
			Datasource datasource `json:"datasource"`
		} `json:"targets"`
	} `json:"panels"`
}

var seriesRE = regexp.MustCompile(`gss_[a-z0-9_]+`)

// TestDashboardQueries checks that the dashboard parses, that every query
// is PromQL on the bundled Prometheus datasource naming only series this
// package exports, and that every metric is on some panel.
func TestDashboardQueries(t *testing.T) {
	b, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatal(err)
	}
	var d dashboard
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatalf("%s: %v", dashboardPath, err)
	}
	if d.Title != "gVisor sandbox scheduler" || d.UID == "" {
		t.Errorf("title %q, uid %q; want title \"gVisor sandbox scheduler\" and a uid", d.Title, d.UID)
	}

	// The series the metrics store holds for each metric.
	series := map[string]string{}
	for _, name := range metricNames {
		if slices.Contains(histogramNames, name) {
			for _, suffix := range []string{"_bucket", "_count", "_sum"} {
				series[name+suffix] = name
			}
		} else {
			series[name] = name
		}
	}
	prom := datasource{Type: "prometheus", UID: "prometheus"}
	shown := map[string]bool{}
	for _, p := range d.Panels {
		if p.Datasource != prom {
			t.Errorf("panel %q: datasource %+v, want %+v", p.Title, p.Datasource, prom)
		}
		if len(p.Targets) == 0 {
			t.Errorf("panel %q has no queries", p.Title)
		}
		for _, q := range p.Targets {
			if q.Datasource != prom {
				t.Errorf("panel %q: query %q datasource %+v, want %+v", p.Title, q.Expr, q.Datasource, prom)
			}
			names := seriesRE.FindAllString(q.Expr, -1)
			if len(names) == 0 {
				t.Errorf("panel %q: query %q names no gss_ series", p.Title, q.Expr)
			}
			for _, s := range names {
				metric, ok := series[s]
				if !ok {
					t.Errorf("panel %q: query %q names %s, which is not exported", p.Title, q.Expr, s)
				}
				shown[metric] = true
			}
		}
	}
	for _, name := range metricNames {
		if !shown[name] {
			t.Errorf("no panel shows %s", name)
		}
	}
}
