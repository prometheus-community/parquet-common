package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/thanos-io/objstore"
)

func TestInstantQuery(t *testing.T) {
	defaultQueryTime := time.Unix(50, 0)
	engine := promql.NewEngine(opts)
	t.Cleanup(func() { engine.Close() })

	cases := []struct {
		load      string
		name      string
		queries   []string
		queryTime time.Time
	}{
		{
			name: "vector selector with empty labels",
			load: `load 10s
			    metric{pod="nginx-1", a=""} 1+2x40`,
			queries: []string{`metric`},
		},
		{
			name: "vector selector with different labelnames",
			load: `load 10s
			    metric{pod="nginx-1", a="foo"} 1+1x40
			    metric{pod="nginx-1", b="bar"} 1+1x40`,
			queries: []string{`metric`},
		},
		{
			name: "float histogram",
			load: `load 5m
				http_requests_histogram{job="api-server", instance="3", group="canary"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}`,
			queries: []string{
				`max(http_requests_histogram)`,
				`min(http_requests_histogram)`,
			},
		},
		{
			name: "vector selector with many labels",
			load: `load 10s
			    metric{pod="nginx-1", a="b", c="d", e="f"} 1+1x40`,
			queries: []string{`metric`},
		},
		{
			name: "vector selector with many not equal comparison",
			load: `load 10s
			    metric{pod="nginx-1", a="b", c="d", e="f"} 1+1x40`,
			queries: []string{`metric{a!="a",c!="c"}`},
		},
		{
			name: "vector selector with regex selector",
			load: `load 10s
			    metric{pod="nginx-1", a="foo"} 1+1x40
			    metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`metric{a=~"f.*"}`},
		},
		{
			name: "vector selector with not regex selector",
			load: `load 10s
                metric{pod="nginx-1", a="foo"} 1+1x40
                metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`metric{a!~"f.*"}`},
		},
		{
			name: "vector selector with not equal selector on unknown column",
			load: `load 10s
                metric{pod="nginx-1"} 1+1x40
                metric{pod="nginx-1"} 1+1x40`,
			queries: []string{`metric{a!~"f.*"}`},
		},
		{
			name: "sum with by grouping",
			load: `load 10s
                metric{pod="nginx-1", a="foo"} 1+1x40
                metric{pod="nginx-2", a="bar"} 1+1x40
                metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`sum by (a) (metric)`},
		},
		{
			name: "sum with without grouping",
			load: `load 10s
                metric{pod="nginx-1", a="foo"} 1+1x40
                metric{pod="nginx-2", a="bar"} 1+1x40
                metric{pod="nginx-1", a="bar"} 1+1x40`,
			queries: []string{`sum without (a) (metric)`},
		},
		{
			name: "sum_over_time with subquery",
			load: `load 10s
			    metric{pod="nginx-1", series="1"} 1+1x40
			    metric{pod="nginx-2", series="2"} 2+2x50
			    metric{pod="nginx-4", series="3"} 5+2x50
			    metric{pod="nginx-5", series="1"} 8+4x50
			    metric{pod="nginx-6", series="2"} 2+3x50`,
			queryTime: time.Unix(600, 0),
			queries:   []string{`sum_over_time(sum by (series) (x)[5m:1m])`},
		},
		{
			name: "",
			load: `load 10s
			    data{test="ten",point="a"} 2
				data{test="ten",point="b"} 8
				data{test="ten",point="c"} 1e+100
				data{test="ten",point="d"} -1e100
				data{test="pos_inf",group="1",point="a"} Inf
				data{test="pos_inf",group="1",point="b"} 2
				data{test="pos_inf",group="2",point="a"} 2
				data{test="pos_inf",group="2",point="b"} Inf
				data{test="neg_inf",group="1",point="a"} -Inf
				data{test="neg_inf",group="1",point="b"} 2
				data{test="neg_inf",group="2",point="a"} 2
				data{test="neg_inf",group="2",point="b"} -Inf
				data{test="inf_inf",point="a"} Inf
				data{test="inf_inf",point="b"} -Inf
				data{test="nan",group="1",point="a"} NaN
				data{test="nan",group="1",point="b"} 2
				data{test="nan",group="2",point="a"} 2
				data{test="nan",group="2",point="b"} NaN`,
			queryTime: time.Unix(60, 0),
			queries:   []string{`sum(data{test="ten"})`},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"} 0+10x10
				http_requests{job="api-server", instance="1", group="production"} 0+20x10
				http_requests{job="api-server", instance="0", group="canary"}   0+30x10
				http_requests{job="api-server", instance="1", group="canary"}   0+40x10
				http_requests{job="app-server", instance="0", group="production"} 0+50x10
				http_requests{job="app-server", instance="1", group="production"} 0+60x10
				http_requests{job="app-server", instance="0", group="canary"}   0+70x10
				http_requests{job="app-server", instance="1", group="canary"}   0+80x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`SUM BY (group) (http_requests{job="api-server"})`,
				`SUM BY (group) (((http_requests{job="api-server"})))`,
				`sum by (group) (http_requests{job="api-server"})`,
				`avg by (group) (http_requests{job="api-server"})`,
				`count by (group) (http_requests{job="api-server"})`,
				`sum without (instance) (http_requests{job="api-server"})`,
				`sum by () (http_requests{job="api-server"})`,
				`sum(http_requests{job="api-server"})`,
				`sum without () (http_requests{job="api-server",group="production"})`,
				`sum without (instance) (http_requests{job="api-server"} or foo)`,
				`sum(http_requests) by (job) + min(http_requests) by (job) + max(http_requests) by (job) + avg(http_requests) by (job)`,
				`sum by (group) (http_requests{job="api-server"})`,
				`sum(sum by (group) (http_requests{job="api-server"})) by (job)`,
				`SUM(http_requests)`,
				`SUM(http_requests{instance="0"}) BY(job)`,
				`SUM(http_requests) BY (job)`,
				`SUM(http_requests) BY (job, nonexistent)`,
				`COUNT(http_requests) BY (job)`,
				`SUM(http_requests) BY (job, group)`,
				`AVG(http_requests) BY (job)`,
				`MIN(http_requests) BY (job)`,
				`MAX(http_requests) BY (job)`,
				`abs(-1 * http_requests{group="production",job="api-server"})`,
				`floor(0.004 * http_requests{group="production",job="api-server"})`,
				`ceil(0.004 * http_requests{group="production",job="api-server"})`,
				`round(0.004 * http_requests{group="production",job="api-server"})`,
				`round(-1 * (0.004 * http_requests{group="production",job="api-server"}))`,
				`round(0.005 * http_requests{group="production",job="api-server"})`,
				`round(-1 * (0.005 * http_requests{group="production",job="api-server"}))`,
				`round(1 + 0.005 * http_requests{group="production",job="api-server"})`,
				`round(-1 * (1 + 0.005 * http_requests{group="production",job="api-server"}))`,
				`round(0.0005 * http_requests{group="production",job="api-server"}, 0.1)`,
				`round(2.1 + 0.0005 * http_requests{group="production",job="api-server"}, 0.1)`,
				`round(5.2 + 0.0005 * http_requests{group="production",job="api-server"}, 0.1)`,
				`round(-1 * (5.2 + 0.0005 * http_requests{group="production",job="api-server"}), 0.1)`,
				`round(0.025 * http_requests{group="production",job="api-server"}, 5)`,
				`round(0.045 * http_requests{group="production",job="api-server"}, 5)`,
				`stddev(http_requests)`,
				`stddev by (instance)(http_requests)`,
				`stdvar(http_requests)`,
				`stdvar by (instance)(http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"} 0+1.33x10
				http_requests{job="api-server", instance="1", group="production"} 0+1.33x10
				http_requests{job="api-server", instance="0", group="canary"} 0+1.33x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`stddev(http_requests)`,
				`stdvar(http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				label_grouping_test{a="aa", b="bb"} 0+10x10
				label_grouping_test{a="a", b="abb"} 0+20x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`sum(label_grouping_test) by (a, b)`,
			},
		},
		{
			name: "",
			load: `load 5m
				label_grouping_test{a="aa", b="bb"} 0+10x10
				label_grouping_test{a="a", b="abb"} 0+20x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`sum(label_grouping_test) by (a, b)`,
			},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"}	1
				http_requests{job="api-server", instance="1", group="production"}	2
				http_requests{job="api-server", instance="0", group="canary"}		NaN
				http_requests{job="api-server", instance="1", group="canary"}		3
				http_requests{job="api-server", instance="2", group="canary"}		4
				http_requests_histogram{job="api-server", instance="3", group="canary"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`max(http_requests)`,
				`min(http_requests)`,
				`max by (group) (http_requests)`,
				`min by (group) (http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				http_requests{job="api-server", instance="0", group="production"}	0+10x10
				http_requests{job="api-server", instance="1", group="production"}	0+20x10
				http_requests{job="api-server", instance="2", group="production"}	NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN
				http_requests{job="api-server", instance="0", group="canary"}		0+30x10
				http_requests{job="api-server", instance="1", group="canary"}		0+40x10
				http_requests{job="app-server", instance="0", group="production"}	0+50x10
				http_requests{job="app-server", instance="1", group="production"}	0+60x10
				http_requests{job="app-server", instance="0", group="canary"}		0+70x10
				http_requests{job="app-server", instance="1", group="canary"}		0+80x10
				foo 3+0x10`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`topk(3, http_requests)`,
				`topk((3), (http_requests))`,
				`topk(5, http_requests{group="canary",job="app-server"})`,
				`bottomk(3, http_requests)`,
				`bottomk(5, http_requests{group="canary",job="app-server"})`,
				`topk by (group) (1, http_requests)`,
				`bottomk by (group) (2, http_requests)`,
				`bottomk by (group) (2, http_requests{group="production"})`,
				`topk(3, http_requests{job="api-server",group="production"})`,
				`bottomk(3, http_requests{job="api-server",group="production"})`,
				`bottomk(9999999999, http_requests{job="app-server",group="canary"})`,
				`topk(9999999999, http_requests{job="api-server",group="production"})`,
				`topk(scalar(foo), http_requests)`,
			},
		},
		{
			name: "",
			load: `load 5m
				version{job="api-server", instance="0", group="production"}	6
				version{job="api-server", instance="1", group="production"}	6
				version{job="api-server", instance="2", group="production"}	6
				version{job="api-server", instance="0", group="canary"}		8
				version{job="api-server", instance="1", group="canary"}		8
				version{job="app-server", instance="0", group="production"}	6
				version{job="app-server", instance="1", group="production"}	6
				version{job="app-server", instance="0", group="canary"}		7
				version{job="app-server", instance="1", group="canary"}		7
				version{job="app-server", instance="2", group="canary"}		{{schema:0 sum:10 count:20 z_bucket_w:0.001 z_bucket:2 buckets:[1 2] n_buckets:[1 2]}}
				version{job="app-server", instance="3", group="canary"}		{{schema:0 sum:10 count:20 z_bucket_w:0.001 z_bucket:2 buckets:[1 2] n_buckets:[1 2]}}`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`count_values("version", version)`,
				`count_values(((("version"))), version)`,
				`count_values without (instance)("version", version)`,
				`count_values without (instance)("job", version)`,
				`count_values by (job, group)("job", version)`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="two samples",point="a"} 0
				data{test="two samples",point="b"} 1
				data{test="three samples",point="a"} 0
				data{test="three samples",point="b"} 1
				data{test="three samples",point="c"} 2
				data{test="uneven samples",point="a"} 0
				data{test="uneven samples",point="b"} 1
				data{test="uneven samples",point="c"} 4
				data_histogram{test="histogram sample", point="c"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}
				foo .8`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`quantile without(point)(0.8, data)`,
				`quantile(0.8, data_histogram)`,
				`quantile without(point)(scalar(foo), data)`,
				`quantile without(point)((scalar(foo)), data)`,
				`quantile without(point)(NaN, data)`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="two samples",point="a"} 0
				data{test="two samples",point="b"} 1
				data{test="three samples",point="a"} 0
				data{test="three samples",point="b"} 1
				data{test="three samples",point="c"} 2
				data{test="uneven samples",point="a"} 0
				data{test="uneven samples",point="b"} 1
				data{test="uneven samples",point="c"} 4
				data{test="histogram sample",point="c"} {{schema:0 sum:0 count:0}}
				foo .8`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`group without(point)(data)`,
				`group(foo)`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="ten",point="a"} 8
				data{test="ten",point="b"} 10
				data{test="ten",point="c"} 12
				data{test="inf",point="a"} 0
				data{test="inf",point="b"} Inf
				data{test="inf",point="d"} Inf
				data{test="inf",point="c"} 0
				data{test="-inf",point="a"} -Inf
				data{test="-inf",point="b"} -Inf
				data{test="-inf",point="c"} 0
				data{test="inf2",point="a"} Inf
				data{test="inf2",point="b"} 0
				data{test="inf2",point="c"} Inf
				data{test="-inf2",point="a"} -Inf
				data{test="-inf2",point="b"} 0
				data{test="-inf2",point="c"} -Inf
				data{test="inf3",point="b"} Inf
				data{test="inf3",point="d"} Inf
				data{test="inf3",point="c"} Inf
				data{test="inf3",point="d"} -Inf
				data{test="-inf3",point="b"} -Inf
				data{test="-inf3",point="d"} -Inf
				data{test="-inf3",point="c"} -Inf
				data{test="-inf3",point="c"} Inf
				data{test="nan",point="a"} -Inf
				data{test="nan",point="b"} 0
				data{test="nan",point="c"} Inf
				data{test="big",point="a"} 9.988465674311579e+307
				data{test="big",point="b"} 9.988465674311579e+307
				data{test="big",point="c"} 9.988465674311579e+307
				data{test="big",point="d"} 9.988465674311579e+307
				data{test="-big",point="a"} -9.988465674311579e+307
				data{test="-big",point="b"} -9.988465674311579e+307
				data{test="-big",point="c"} -9.988465674311579e+307
				data{test="-big",point="d"} -9.988465674311579e+307
				data{test="bigzero",point="a"} -9.988465674311579e+307
				data{test="bigzero",point="b"} -9.988465674311579e+307
				data{test="bigzero",point="c"} 9.988465674311579e+307
				data{test="bigzero",point="d"} 9.988465674311579e+307`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`avg(data{test="ten"})`,
				`avg(data{test="inf"})`,
				`avg(data{test="inf2"})`,
				`avg(data{test="inf3"})`,
				`avg(data{test="-inf"})`,
				`avg(data{test="-inf2"})`,
				`avg(data{test="-inf3"})`,
				`avg(data{test="nan"})`,
				`avg(data{test="big"})`,
				`avg(data{test="-big"})`,
				`avg(data{test="bigzero"})`,
			},
		},
		{
			name: "",
			load: `load 10s
				data{test="ten",point="a"} 2
				data{test="ten",point="b"} 8
				data{test="ten",point="c"} 1e+100
				data{test="ten",point="d"} -1e100
				data{test="pos_inf",group="1",point="a"} Inf
				data{test="pos_inf",group="1",point="b"} 2
				data{test="pos_inf",group="2",point="a"} 2
				data{test="pos_inf",group="2",point="b"} Inf
				data{test="neg_inf",group="1",point="a"} -Inf
				data{test="neg_inf",group="1",point="b"} 2
				data{test="neg_inf",group="2",point="a"} 2
				data{test="neg_inf",group="2",point="b"} -Inf
				data{test="inf_inf",point="a"} Inf
				data{test="inf_inf",point="b"} -Inf
				data{test="nan",group="1",point="a"} NaN
				data{test="nan",group="1",point="b"} 2
				data{test="nan",group="2",point="a"} 2
				data{test="nan",group="2",point="b"} NaN`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`sum(data{test="ten"})`,
				`avg(data{test="ten"})`,
				`sum by (group) (data{test="pos_inf"})`,
				`avg by (group) (data{test="pos_inf"})`,
				`sum by (group) (data{test="neg_inf"})`,
				`avg by (group) (data{test="neg_inf"})`,
				`sum(data{test="inf_inf"})`,
				`avg(data{test="inf_inf"})`,
				`sum by (group) (data{test="nan"})`,
				`avg by (group) (data{test="nan"})`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} 1
				series{label="b"} 2
				series{label="c"} NaN`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} NaN
				series{label="b"} 1
				series{label="c"} 2`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series NaN`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} 1
				series{label="b"} 2
				series{label="c"} inf`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series{label="a"} inf
				series{label="b"} 1
				series{label="c"} 2`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
				`stddev by (label) (series)`,
				`stdvar by (label) (series)`,
			},
		},
		{
			name: "",
			load: `load 5m
				series inf`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`stddev(series)`,
				`stdvar(series)`,
			},
		},
		{
			name: "",
			load: `load 1s
				node_namespace_pod:kube_pod_info:{namespace="observability",node="gke-search-infra-custom-96-253440-fli-d135b119-jx00",pod="node-exporter-l454v"} 1
				node_cpu_seconds_total{cpu="10",endpoint="https",instance="10.253.57.87:9100",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v",service="node-exporter"} 449
				node_cpu_seconds_total{cpu="35",endpoint="https",instance="10.253.57.87:9100",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v",service="node-exporter"} 449
				node_cpu_seconds_total{cpu="89",endpoint="https",instance="10.253.57.87:9100",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v",service="node-exporter"} 449`,
			queryTime: time.Unix(4, 0),
			queries: []string{
				`count by(namespace, pod, cpu) (node_cpu_seconds_total{cpu=~".*",job="node-exporter",mode="idle",namespace="observability",pod="node-exporter-l454v"}) * on(namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{namespace="observability",pod="node-exporter-l454v"}`,
			},
		},
		{
			name: "duplicate labelset in promql output",
			load: `load 5m
				testmetric1{src="a",dst="b"} 0
				testmetric2{src="a",dst="b"} 1`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				// TODO(kdeems): re-enable when regex name match is supported
				// `ceil({__name__=~'testmetric1|testmetric2'})`,
			},
		},
		{
			name: "operators",
			load: `load 5m
				http_requests_total{job="api-server", instance="0", group="production"}	0+10x10
				http_requests_total{job="api-server", instance="1", group="production"}	0+20x10
				http_requests_total{job="api-server", instance="0", group="canary"}	0+30x10
				http_requests_total{job="api-server", instance="1", group="canary"}	0+40x10
				http_requests_total{job="app-server", instance="0", group="production"}	0+50x10
				http_requests_total{job="app-server", instance="1", group="production"}	0+60x10
				http_requests_total{job="app-server", instance="0", group="canary"}	0+70x10
				http_requests_total{job="app-server", instance="1", group="canary"}	0+80x10
				http_requests_histogram{job="app-server", instance="1", group="production"} {{schema:1 sum:15 count:10 buckets:[3 2 5 7 9]}}x11

				load 5m
					vector_matching_a{l="x"} 0+1x100
					vector_matching_a{l="y"} 0+2x50
					vector_matching_b{l="x"} 0+4x25`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`SUM(http_requests_total) BY (job) - COUNT(http_requests_total) BY (job)`,
				`2 - SUM(http_requests_total) BY (job)`,
				`-http_requests_total{job="api-server",instance="0",group="production"}`,
				`+http_requests_total{job="api-server",instance="0",group="production"}`,
				`- - - SUM(http_requests_total) BY (job)`,
				`- - - 1`,
				`-2^---1*3`,
				`2/-2^---1*3+2`,
				`-10^3 * - SUM(http_requests_total) BY (job) ^ -1`,
				`1000 / SUM(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) - 2`,
				`SUM(http_requests_total) BY (job) % 3`,
				`SUM(http_requests_total) BY (job) % 0.3`,
				`SUM(http_requests_total) BY (job) ^ 2`,
				`SUM(http_requests_total) BY (job) % 3 ^ 2`,
				`SUM(http_requests_total) BY (job) % 2 ^ (3 ^ 2)`,
				`SUM(http_requests_total) BY (job) % 2 ^ 3 ^ 2`,
				`SUM(http_requests_total) BY (job) % 2 ^ 3 ^ 2 ^ 2`,
				`COUNT(http_requests_total) BY (job) ^ COUNT(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) / 0`,
				`http_requests_total{group="canary", instance="0", job="api-server"} / 0`,
				`-1 * http_requests_total{group="canary", instance="0", job="api-server"} / 0`,
				`0 * http_requests_total{group="canary", instance="0", job="api-server"} / 0`,
				`0 * http_requests_total{group="canary", instance="0", job="api-server"} % 0`,
				`SUM(http_requests_total) BY (job) + SUM(http_requests_total) BY (job)`,
				`(SUM((http_requests_total)) BY (job)) + SUM(http_requests_total) BY (job)`,
				`http_requests_total{job="api-server", group="canary"}`,
				`rate(http_requests_total[25m]) * 25 * 60`,
				`(rate((http_requests_total[25m])) * 25) * 60`,
				`http_requests_total{group="canary"} and http_requests_total{instance="0"}`,
				`(http_requests_total{group="canary"} + 1) and http_requests_total{instance="0"}`,
				`(http_requests_total{group="canary"} + 1) and on(instance, job) http_requests_total{instance="0", group="production"}`,
				`(http_requests_total{group="canary"} + 1) and on(instance) http_requests_total{instance="0", group="production"}`,
				`(http_requests_total{group="canary"} + 1) and ignoring(group) http_requests_total{instance="0", group="production"}`,
				`(http_requests_total{group="canary"} + 1) and ignoring(group, job) http_requests_total{instance="0", group="production"}`,
				`http_requests_total{group="canary"} or http_requests_total{group="production"}`,
				`(http_requests_total{group="canary"} + 1) or http_requests_total{instance="1"}`,
				`(http_requests_total{group="canary"} + 1) or on(instance) (http_requests_total or cpu_count or vector_matching_a)`,
				`(http_requests_total{group="canary"} + 1) or ignoring(l, group, job) (http_requests_total or cpu_count or vector_matching_a)`,
				`http_requests_total{group="canary"} unless http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} unless on(job) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} unless on(job, instance) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} / on(instance,job) http_requests_total{group="production"}`,
				`http_requests_total{group="canary"} unless ignoring(group, instance) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} unless ignoring(group) http_requests_total{instance="0"}`,
				`http_requests_total{group="canary"} / ignoring(group) http_requests_total{group="production"}`,
				`http_requests_total AND ON (dummy) vector(1)`,
				`http_requests_total AND IGNORING (group, instance, job) vector(1)`,
				`SUM(http_requests_total) BY (job) > 1000`,
				`1000 < SUM(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) <= 1000`,
				`SUM(http_requests_total) BY (job) != 1000`,
				`SUM(http_requests_total) BY (job) == 1000`,
				`SUM(http_requests_total) BY (job) == bool 1000`,
				`SUM(http_requests_total) BY (job) == bool SUM(http_requests_total) BY (job)`,
				`SUM(http_requests_total) BY (job) != bool SUM(http_requests_total) BY (job)`,
				`0 == bool 1`,
				`1 == bool 1`,
				`http_requests_total{job="api-server", instance="0", group="production"} == bool 100`,
			},
		},
		{
			name: "wrong query time",
			load: `load 5m
				  node_var{instance="abc",job="node"} 2
				  node_role{instance="abc",job="node",role="prometheus"} 1`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`node_role * on (instance) group_right (role) node_var`,
			},
		},
		{
			name: "operators",
			load: `load 5m
				  node_var{instance="abc",job="node"} 2
				  node_role{instance="abc",job="node",role="prometheus"} 1

				load 5m
				  node_cpu{instance="abc",job="node",mode="idle"} 3
				  node_cpu{instance="abc",job="node",mode="user"} 1
				  node_cpu{instance="def",job="node",mode="idle"} 8
				  node_cpu{instance="def",job="node",mode="user"} 2

				load 5m
				  random{foo="bar"} 1

				load 5m
				  threshold{instance="abc",job="node",target="a@b.com"} 0`,
			queryTime: time.Unix(60, 0),
			queries: []string{
				`node_role * on (instance) group_right (role) node_var`,
				`node_var * on (instance) group_left (role) node_role`,
				`node_var * ignoring (role) group_left (role) node_role`,
				`node_role * ignoring (role) group_right (role) node_var`,
				`node_cpu * ignoring (role, mode) group_left (role) node_role`,
				`node_cpu * on (instance) group_left (role) node_role`,
				`node_cpu / on (instance) group_left sum by (instance,job)(node_cpu)`,
				`sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu)`,
				`sum(sum by (mode, job)(node_cpu) / on (job) group_left sum by (job)(node_cpu))`,
				`node_cpu / ignoring (mode) group_left sum without (mode)(node_cpu)`,
				`node_cpu / ignoring (mode) group_left(dummy) sum without (mode)(node_cpu)`,
				`sum without (instance)(node_cpu) / ignoring (mode) group_left sum without (instance, mode)(node_cpu)`,
				`sum(sum without (instance)(node_cpu) / ignoring (mode) group_left sum without (instance, mode)(node_cpu))`,
				`node_cpu + on(dummy) group_left(foo) random*0`,
				`node_cpu > on(job, instance) group_left(target) threshold`,
				`node_cpu > on(job, instance) group_left(target) (threshold or on (job, instance) (sum by (job, instance)(node_cpu) * 0 + 1))`,
				`node_cpu + 2`,
				`node_cpu - 2`,
				`node_cpu / 2`,
				`node_cpu * 2`,
				`node_cpu ^ 2`,
				`node_cpu % 2`,
			},
		},
		{
			name: "at_modifiers",
			load: `load 10s
				  metric{job="1"} 0+1x1000
				  metric{job="2"} 0+2x1000

				load 1ms
				  metric_ms 0+1x10000`,
			queryTime: time.Unix(10, 0),
			queries: []string{
				`metric @ 100`,
				`metric @ 100s`,
				`metric @ 1m40s`,
				`metric @ 100 offset 50s`,
				`metric @ 100 offset 50`,
				`metric offset 50s @ 100`,
				`metric offset 50 @ 100`,
				`metric @ 0 offset -50s`,
				`metric @ 0 offset -50`,
				`metric offset -50s @ 0`,
				`metric offset -50 @ 0`,
				`metric @ 0 offset -50s`,
				`metric @ 0 offset -50`,
				`-metric @ 100`,
				`---metric @ 100`,
				`minute(metric @ 1500)`,
			},
		},
		{
			name: "at_modifiers",
			load: `load 10s
				  metric{job="1"} 0+1x1000
				  metric{job="2"} 0+2x1000

				load 1ms
				  metric_ms 0+1x10000`,
			queryTime: time.Unix(25, 0),
			queries: []string{
				`sum_over_time(metric{job="1"}[100s] @ 100)`,
				`sum_over_time(metric{job="1"}[100s] @ 100 offset 50s)`,
				`sum_over_time(metric{job="1"}[100s] offset 50s @ 100)`,
				`sum_over_time(metric{job="1"}[100] @ 100 offset 50)`,
				`sum_over_time(metric{job="1"}[100] offset 50s @ 100)`,
				`metric{job="1"} @ 50 + metric{job="1"} @ 100`,
				`sum_over_time(metric{job="1"}[100s] @ 100) + label_replace(sum_over_time(metric{job="2"}[100s] @ 100), "job", "1", "", "")`,
				`sum_over_time(metric{job="1"}[100] @ 100) + label_replace(sum_over_time(metric{job="2"}[100] @ 100), "job", "1", "", "")`,
				`sum_over_time(metric{job="1"}[100s:1s] @ 100)`,
				`sum_over_time(metric{job="1"}[100s:1s] @ 100 offset 20s)`,
				`sum_over_time(metric{job="1"}[100s:1s] offset 20s @ 100)`,
				`sum_over_time(metric{job="1"}[100:1] offset 20 @ 100)`,
			},
		},
		{
			name: "at_modifiers",
			load: `load 10s
				  metric{job="1"} 0+1x1000
				  metric{job="2"} 0+2x1000

				load 1ms
				  metric_ms 0+1x10000`,
			queryTime: time.Unix(0, 0),
			queries: []string{
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[100s] @ 100)[100s:25s] @ 50)[3s:1s] @ 3000)`,
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[10s])[100s:25s] @ 50)[3s:1s] @ 200)`,
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[10s])[100s:25s] @ 200)[3s:1s] @ 50)`,
				`sum_over_time(sum_over_time(sum_over_time(metric{job="1"}[20s])[20s:10s] offset 10s)[100s:25s] @ 1000)`,
				`sum_over_time(minute(metric @ 1500)[100s:10s])`,
				`sum_over_time(minute()[50m:1m] @ 6000)`,
				`sum_over_time(minute()[50m:1m] @ 6000 offset 5m)`,
				`sum_over_time(vector(time())[100s:1s] @ 3000)`,
				`sum_over_time(vector(time())[100s:1s] @ 3000 offset 600s)`,
				`sum_over_time(timestamp(metric{job="1"} @ 10)[100s:10s] @ 3000)`,
				`sum_over_time(timestamp(timestamp(metric{job="1"} @ 999))[10s:1s] @ 10)`,
			},
		},
		{
			name: "literals",
			load: `load 5m
					metric	60 120 180`,
			queryTime: time.Unix(3000, 0),
			queries: []string{
				`12.34e6`,
				`12.34e+6`,
				`12.34e-6`,
				`1+1`,
				`1-1`,
				`1 - -1`,
				`.2`,
				`+0.2`,
				`-0.2e-6`,
				`+Inf`,
				`inF`,
				`-inf`,
				`NaN`,
				`nan`,
				`2.`,
				`1 / 0`,
				`((1) / (0))`,
				`-1 / 0`,
				`0 / 0`,
				`1 % 0`,
			},
		},
	}
	for _, tc := range cases {

		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			queryTime := defaultQueryTime
			if !tc.queryTime.IsZero() {
				queryTime = tc.queryTime
			}

			t.Parallel()
			testStorage := promqltest.LoadedStorage(t, tc.load)
			t.Cleanup(func() { testStorage.Close() })

			db := storageToDB(t, testStorage)
			ctx := context.Background()

			for _, query := range tc.queries {
				t.Run(query, func(t *testing.T) {
					t.Parallel()

					q1, err := engine.NewInstantQuery(ctx, db.Queryable(), nil, query, queryTime)
					if err != nil {
						t.Fatalf("error for query `%s`: %v", query, err)
					}
					defer q1.Close()

					newResult := q1.Exec(ctx)

					q2, err := engine.NewInstantQuery(ctx, testStorage, nil, query, queryTime)
					if err != nil {
						t.Fatalf("error for query `%s`: %v", query, err)
					}
					defer q2.Close()

					oldResult := q2.Exec(ctx)

					if !cmp.Equal(oldResult, newResult, comparer) {
						t.Fatalf("query `%s`: expected results to be equal, \nOLD:\n%s\nNEW:\n%s", query, oldResult, newResult)
					}
				})
			}
		})
	}
}
