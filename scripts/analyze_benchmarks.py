#!/usr/bin/env python3
"""Regenerate tables, figures and the performance report from archived Go output."""
import argparse, csv, gzip, json, math, os, statistics
from collections import defaultdict
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR', '/tmp/rpc-handler-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parents[1]
LABELS={'hash':'Hash · 4 KiB','sort':'Sort · 2,048 int32','matrix':'Matrix · 32×32','zlib_random':'zlib · random 4 KiB','mixed':'Equal-weight mix','zlib_repeating':'zlib · repeating 4 KiB','lz4_random':'LZ4 · random 4 KiB','lz4_repeating':'LZ4 · repeating 4 KiB'}
PRIMARY=['hash','sort','matrix','zlib_random','mixed']


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',default='results/step9');args=parser.parse_args()
    root=ROOT/args.root
    groups=defaultdict(list)
    for path in sorted(root.glob('measure-*/result.json')):
        row=json.loads(path.read_text());groups[(row['scenario'],row['rate'])].append(row)
    expected=json.loads((root/'measurement-order.json').read_text())
    actual={(r['scenario'],r['rate'],r['repeat']) for rows in groups.values() for r in rows}
    if actual!={tuple(t) for t in expected}:raise RuntimeError('Incomplete measurements; do not produce final report yet')
    # Validate archival integrity and counts independently of the aggregate CSV.
    total_rows=0
    for path in root.glob('measure-*/result.json'):
        result=json.loads(path.read_text());counts=defaultdict(int);latencies=[];completions=0
        with gzip.open(path.parent/'requests.csv.gz','rt') as stream:
            for row in csv.DictReader(stream):
                counts[row['outcome']]+=1;total_rows+=1
                if row['outcome']=='success':
                    latencies.append(int(row['latency_ns'])/1e6)
                    if int(row['completed_ns'])<30_000_000_000:completions+=1
        summary=result['summary']
        assert sum(counts.values())==summary['scheduled'],path
        assert counts['success']==summary['successes'],path
        assert abs(completions/30-summary['throughput'])<1e-9,path
        latencies.sort()
        if latencies:assert abs(latencies[math.ceil(.95*len(latencies))-1]-summary['p95_ms'])<1e-7,path
    aggregate=[]
    for (scenario,rate),rows in sorted(groups.items()):
        assert len(rows)==3
        r={'scenario':scenario,'rate':rate,'repeats':len(rows),'passes_all':all(x['passes_threshold'] for x in rows)}
        for key in ['mean_ms','p50_ms','p95_ms','p99_ms','throughput','mean_scheduling_delay_ms']:
            values=[x['summary'][key] for x in rows]
            r[key]=statistics.mean(values);r[key+'_min']=min(values);r[key+'_max']=max(values)
        for name,keys in [('error_pct',['errors','timeouts','incomplete']),('missed_pct',['missed'])]:
            r[name]=statistics.mean(100*sum(x['summary'][k] for k in keys)/x['summary']['scheduled'] for x in rows)
        r['scheduling_p95_ms']=statistics.mean(x['scheduling_p95_ms'] for x in rows)
        r['scheduling_p95_ms_max']=max(x['scheduling_p95_ms'] for x in rows)
        r['warmup_incomplete']=0
        for x in rows:
            meta=json.loads((root/f"measure-{scenario}-r{rate}-rep{x['repeat']}"/'run.json').read_text())
            warm=meta['WarmupSummary'];r['warmup_incomplete']+=warm['incomplete']+warm['timeouts']
        aggregate.append(r)
    with (root/'aggregate.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(aggregate[0]));writer.writeheader();writer.writerows(aggregate)
    figures=root/'figures';figures.mkdir(exist_ok=True)
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'figure.dpi':140})
    def save(fig,name):
        fig.savefig(figures/(name+'.png'),bbox_inches='tight');fig.savefig(figures/(name+'.svg'),bbox_inches='tight');plt.close(fig)
    def panels(title,filename,draw,ylabel):
        fig,axes=plt.subplots(2,3,figsize=(13,7.5));axes=axes.flat
        for ax,scenario in zip(axes,PRIMARY):
            rows=sorted([r for r in aggregate if r['scenario']==scenario],key=lambda x:x['rate'])
            draw(ax,rows);ax.set_title(LABELS[scenario]);ax.set_xscale('log');ax.set_xlabel('Offered requests/s');ax.set_ylabel(ylabel);ax.grid(alpha=.2)
        axes[5].axis('off')
        fig.suptitle(title+'\n30-second windows · 3 repeats · same-host loopback',fontsize=14)
        fig.tight_layout(rect=(0,0,1,.93));save(fig,filename)
    def latency(ax,rows):
        x=[r['rate'] for r in rows]
        for key,label,color in [('mean_ms','Mean','#1769aa'),('p95_ms','p95','#d35400')]:
            y=[r[key] for r in rows]
            ax.plot(x,y,'o-',label=label,color=color)
            ax.fill_between(x,[r[key+'_min'] for r in rows],[r[key+'_max'] for r in rows],alpha=.13,color=color)
        ax.axhline(50,color='gray',ls=':',label='50 ms threshold');ax.set_yscale('log');ax.legend(fontsize=8)
    panels('Load–latency curves (bands: repeat minimum–maximum)','load-latency',latency,'Successful-call latency (ms)')
    def throughput(ax,rows):
        x=[r['rate'] for r in rows];ax.plot(x,x,':',color='gray',label='Ideal')
        ax.plot(x,[r['throughput'] for r in rows],'o-',label='Achieved',color='#1769aa')
        ax.fill_between(x,[r['throughput_min'] for r in rows],[r['throughput_max'] for r in rows],alpha=.15)
        ax.legend(fontsize=8)
    panels('Throughput within the measurement window','throughput',throughput,'Successful completions/s')
    def failures(ax,rows):
        x=[r['rate'] for r in rows]
        ax.plot(x,[r['error_pct'] for r in rows],'o-',label='Errors + timeouts + incomplete')
        ax.plot(x,[r['missed_pct'] for r in rows],'s-',label='Generator misses')
        ax.set_ylim(bottom=-.5);ax.legend(fontsize=7)
    panels('Failure and missed-arrival rates (denominator: offered arrivals)','failures',failures,'Percent of scheduled arrivals')
    def scheduling(ax,rows):
        x=[r['rate'] for r in rows]
        ax.plot(x,[r['scheduling_p95_ms'] for r in rows],'o-',label='Mean repeat p95')
        ax.plot(x,[r['scheduling_p95_ms_max'] for r in rows],'s--',label='Worst repeat p95')
        ax.axhline(5,color='gray',ls=':',label='5 ms threshold');ax.set_yscale('log');ax.legend(fontsize=8)
    panels('Generator scheduling delay','scheduling-delay',scheduling,'Scheduling delay (ms)')
    fig,axes=plt.subplots(1,2,figsize=(13,5))
    for ax,scenarios,title in [(axes[0],PRIMARY,'Operation comparison at 100 requests/s'),(axes[1],['zlib_random','zlib_repeating','lz4_random','lz4_repeating'],'Compression comparison at 100 requests/s')]:
        rows=[next(r for r in aggregate if r['scenario']==s and r['rate']==100) for s in scenarios]
        x=list(range(len(rows)))
        ax.bar([v-.18 for v in x],[r['mean_ms'] for r in rows],.36,label='Mean',color='#1769aa')
        ax.bar([v+.18 for v in x],[r['p95_ms'] for r in rows],.36,label='p95',color='#d35400')
        ax.set_xticks(x,[LABELS[s] for s in scenarios],rotation=25,ha='right');ax.set_ylabel('Successful-call latency (ms)');ax.set_title(title);ax.legend();ax.grid(axis='y',alpha=.2)
    fig.tight_layout();save(fig,'comparison')
    env=json.loads((root/'environment.json').read_text())
    report=['# RPC performance report','', '## Result and scope','',f'This study contains {len(expected)} measured runs, each with a 30-second window and three repetitions per configuration. The archived raw data contains {total_rows:,} scheduled arrivals. Pilot runs selected the measured rates and are excluded from the reported statistics.','',
    'These are end-to-end results for this implementation on one shared machine. The highest passing tested rate is a measured operating point, not a precise maximum server capacity. Both generator and server consume CPU on the same host.','',
    '## Methodology','',f"- Hardware: {env['cpu_model']}; {env['hardware_model']}; {env['logical_cpus']} logical CPUs; {int(env['memory_bytes'])/2**30:.0f} GiB RAM.",f"- OS: {env['platform']}. Runtime: {env['go']}.",'- Separate Go server and load-generator processes on loopback; `GOMAXPROCS=4` per process. Runs execute sequentially in a fixed shuffled order (seed 151). Each run starts a fresh server.',
    '- Server: 4 workers, queue 128, connection cap 16, read timeout 30 s, write timeout 2 s. Client: 4 persistent connections, global in-flight cap 512, timeout/drain 2 s, retries disabled.',
    '- Windows: 2 s warm-up followed by 30 s measured load; seeds 1, 2, 3 across repetitions; 8 pre-generated inputs per operation. Verbose logging disabled; error logs remain enabled and are archived.',
    '- Inputs: 4 KiB bytes for hashing/compression, 2,048 signed int32 values for sorting, two 32×32 float64 matrices. The mixed workload uses equal operation weights and zlib on pseudorandom bytes. Compression comparisons include pseudorandom and repeating `abcd` input.',
    '- Latency begins before client enqueue/serialization and ends when the generator observes the terminal response. Percentiles use successful calls only, including drain completions. Throughput excludes drain completions. Failure/miss denominators are all scheduled arrivals.',
    '- Plotted lines are means of three run-level statistics; shaded bands show their minimum and maximum. Averaged p95 values are not pooled percentiles or confidence intervals.',
    '', '## Prespecified sustainability criteria','', 'Every repeat must have successful-call p95 ≤ 50 ms, total unsuccessful offered arrivals ≤ 1%, generator misses ≤ 0.1%, scheduling-delay p95 ≤ 5 ms, and successful in-window throughput ≥ 98% of offered rate. These thresholds were recorded before pilots. They assess the complete test setup, including generator fidelity.','',
    '| Workload | Highest passing tested rate | Mean achieved throughput there | First failing tested rate |','| --- | ---: | ---: | ---: |']
    for scenario in PRIMARY:
        rows=sorted([r for r in aggregate if r['scenario']==scenario],key=lambda r:r['rate'])
        passing=[r for r in rows if r['passes_all']];failing=[r for r in rows if not r['passes_all']]
        best=max(passing,key=lambda r:r['rate']) if passing else None
        report.append(f"| {LABELS[scenario]} | {str(best['rate'])+' req/s' if best else 'None tested'} | {format(best['throughput'],'.1f')+' req/s' if best else '—'} | {str(failing[0]['rate'])+' req/s' if failing else 'Not reached'} |")
    report += ['', 'A passing point above a failing point indicates run variability rather than a monotonic capacity boundary; inspect all repetitions in `aggregate.csv`. A failing rate may be limited by scheduling delay even when server errors are zero.','', '## Measurements','',
    '| Workload | Rate | Mean ms | p95 ms | Throughput/s | Errors/timeouts/incomplete % | Missed % | Worst scheduling p95 ms | All repeats pass |',
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |']
    for r in aggregate:report.append(f"| {LABELS[r['scenario']]} | {r['rate']} | {r['mean_ms']:.3f} | {r['p95_ms']:.3f} | {r['throughput']:.1f} | {r['error_pct']:.3f} | {r['missed_pct']:.3f} | {r['scheduling_p95_ms_max']:.3f} | {'Yes' if r['passes_all'] else 'No'} |")
    for title,name in [('Load–latency','load-latency'),('Throughput','throughput'),('Failures and misses','failures'),('Generator fidelity','scheduling-delay'),('Operation and compression comparisons','comparison')]:
        report += ['',f'### {title}','',f'![{title}](../{args.root}/figures/{name}.png)']
    report += ['', '## Interpretation and limitations','',
    '- Rising response latency with throughput flattening is consistent with queueing and resource saturation, but shared-host CPU contention and JSON serialization also contribute. Generator scheduling delay and misses must be considered alongside the latency curve.',
    '- Low successful-call latency at overload does not mean the offered workload was served: unsuccessful and missed requests are excluded from latency percentiles and shown separately.',
    '- Operation comparisons use different input contracts and fixed sizes, not equal work. Compression format, compressibility, and Go allocation/encoding overhead all affect end-to-end time; these measurements do not isolate algorithm-only speed.',
    f"- Warm-up timeouts/incomplete calls across measured runs: {sum(r['warmup_incomplete'] for r in aggregate)}. Such calls may leave remote work in flight because cancellation does not stop computation; this can affect the next phase.",
    '- This laptop was not CPU-pinned or isolated from other applications. Thermal behavior, OS scheduling, and background work may affect repetitions. The test records machine details but not per-process CPU utilization; attribution to a single bottleneck is therefore an inference.',
    '- The rate grid is sparse and adaptively selected from short pilots. A longer run, finer rate grid, separate generator host, and CPU profiling would improve capacity estimates. Compression variants beyond zlib/random are compared at 100 requests/s only; their saturation points were not measured.',
    '', '## Reproduction and artifacts','',
    'The RPC implementation and load generator are Go. Python only orchestrates processes, archives CSV, and plots results. Matplotlib 3.10.1 was used for figures. Source SHA-256 hashes, hardware details and thresholds are in `environment.json`; run order is in `measurement-order.json`; each run retains exact command arguments. Raw per-request CSV is losslessly gzip-compressed.',
    '', '```sh','go build -o bin/rpc-server ./cmd/server','go build -o bin/rpc-loadgen ./cmd/loadgen','python3 scripts/benchmark.py --phase pilot --out results/new-study','python3 scripts/benchmark.py --phase measure --out results/new-study','python3 scripts/analyze_benchmarks.py --root results/new-study','```','',
    'To regenerate only this study’s figures and report:', '', '```sh','python3 scripts/analyze_benchmarks.py --root results/step9','```','',
    f'Artifacts: [aggregate table](../{args.root}/aggregate.csv), [environment](../{args.root}/environment.json), [selected rates](../{args.root}/selected-rates.json). PNG and SVG figures are both retained. Analysis independently checks raw row counts, successful throughput and p95 against the Go summaries.']
    (ROOT/'docs/PERFORMANCE.md').write_text('\n'.join(report)+'\n')
    print(f'Validated {total_rows:,} raw rows from {len(expected)} runs; wrote report and five PNG/SVG figures.')
if __name__=='__main__':main()
