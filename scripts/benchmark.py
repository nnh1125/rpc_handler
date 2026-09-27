#!/usr/bin/env python3
"""Orchestrate the Go benchmark binaries; archive raw CSVs without losing rows."""
import argparse, csv, gzip, hashlib, json, os, platform, random, signal, subprocess, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = {
    'hash': ['-mix', 'hash_compute:1'],
    'sort': ['-mix', 'sort_array:1'],
    'matrix': ['-mix', 'matrix_multiply:1'],
    'zlib_random': ['-mix', 'compress_data:1', '-algorithm', 'zlib'],
    'mixed': ['-mix', 'hash_compute:1,sort_array:1,matrix_multiply:1,compress_data:1', '-algorithm', 'zlib'],
    'zlib_repeating': ['-mix', 'compress_data:1', '-algorithm', 'zlib', '-compressible'],
    'lz4_random': ['-mix', 'compress_data:1', '-algorithm', 'lz4'],
    'lz4_repeating': ['-mix', 'compress_data:1', '-algorithm', 'lz4', '-compressible'],
}
PRIMARY = ['hash', 'sort', 'matrix', 'zlib_random', 'mixed']
ENV = dict(os.environ, GOMAXPROCS='4')

def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')

def inspect_run(path):
    data = json.loads((path / 'run.json').read_text())
    s = data['Summaries'][0]
    delays = []
    with (path / 'requests.csv').open() as f:
        for row in csv.DictReader(f):
            if int(row['send_ns']) >= 0:
                delays.append(int(row['scheduling_delay_ns']) / 1e6)
    delays.sort()
    import math
    p95 = delays[max(0, math.ceil(.95 * len(delays)) - 1)] if delays else 0
    failed = s['errors'] + s['timeouts'] + s['incomplete'] + s['missed']
    # Prespecified criteria, applied to each measured repeat separately.
    passed = (s['p95_ms'] <= 50 and failed <= .01*s['scheduled']
              and s['missed'] <= .001*s['scheduled'] and p95 <= 5
              and s['throughput'] >= .98*data['Config']['Rate']
              and not data['Interrupted'])
    return {'summary': s, 'scheduling_p95_ms': p95, 'passes_threshold': passed}

def execute(root, phase, scenario, rate, repeat, duration):
    name = f'{phase}-{scenario}-r{rate}-rep{repeat}'
    path = root / name
    if (path / 'result.json').exists():
        return json.loads((path / 'result.json').read_text())
    if path.exists():
        raise RuntimeError(f'Incomplete directory {path}; preserve and use a new root')
    log_path = root / (name + '-server.log')
    server_cmd = [str(ROOT/'bin/rpc-server'), '-addr', '127.0.0.1:0', '-workers', '4', '-queue-size', '128', '-max-connections', '16', '-read-timeout', '30s', '-write-timeout', '2s', '-shutdown-timeout', '3s']
    server = subprocess.Popen(server_cmd, cwd=ROOT, env=ENV, stderr=subprocess.PIPE, text=True)
    try:
        startup = server.stderr.readline()
        if 'RPC server listening on ' not in startup:
            raise RuntimeError(startup)
        address = startup.strip().split('RPC server listening on ', 1)[1]
        # Continuously drain logs to disk so pipe backpressure cannot stall server.
        import threading
        def logs():
            with log_path.open('w') as out:
                out.write(startup)
                for line in server.stderr: out.write(line)
        thread = threading.Thread(target=logs)
        thread.start()
        command = [str(ROOT/'bin/rpc-loadgen'), '-addr', address, '-rate', str(rate), '-duration', duration, '-warmup', '1s' if phase=='pilot' else '2s', '-drain', '2s', '-timeout', '2s', '-connections', '4', '-max-in-flight', '512', '-bytes', '4096', '-sort-size', '2048', '-matrix-n', '32', '-samples', '8', '-seed', str(repeat), '-out', str(path)] + SCENARIOS[scenario]
        print(f'START {name} duration={duration}', flush=True)
        started = time.time()
        process = subprocess.run(command, cwd=ROOT, env=ENV, capture_output=True, text=True, timeout=90)
        if path.exists():
            (path/'console.txt').write_text(process.stdout + process.stderr)
            save(path/'commands.json', {'server':server_cmd, 'loadgen':command, 'GOMAXPROCS':'4'})
        if process.returncode: raise RuntimeError(process.stdout+process.stderr)
        result = inspect_run(path)
        result.update(phase=phase, scenario=scenario, rate=rate, repeat=repeat, duration=duration, elapsed_s=time.time()-started)
        save(path/'result.json', result)
        # Keep all raw data, losslessly compressed to conserve local disk space.
        raw = path/'requests.csv'
        with raw.open('rb') as src, gzip.open(path/'requests.csv.gz','wb') as dst:
            import shutil
            shutil.copyfileobj(src,dst)
        raw.unlink()
        s=result['summary']
        print(f"DONE {name} throughput={s['throughput']:.1f} p95={s['p95_ms']:.2f}ms misses={s['missed']} errors={s['errors']} sched_p95={result['scheduling_p95_ms']:.2f}ms pass={result['passes_threshold']}",flush=True)
        return result
    finally:
        server.send_signal(signal.SIGTERM)
        try: server.wait(timeout=5)
        except subprocess.TimeoutExpired: server.kill(); server.wait()
        if 'thread' in locals(): thread.join(timeout=5)
        server.stderr.close()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--out',default='results/step9')
    parser.add_argument('--phase',choices=['pilot','measure'],required=True)
    args=parser.parse_args()
    root=(ROOT/args.out).resolve();root.mkdir(parents=True,exist_ok=True)
    if not (root/'environment.json').exists():
        def command(*args):
            p=subprocess.run(args,capture_output=True,text=True)
            return p.stdout.strip() or p.stderr.strip()
        files=list((ROOT/'client').glob('*.go'))+list((ROOT/'internal').rglob('*.go'))+list((ROOT/'cmd').rglob('*.go'))+[ROOT/'go.mod',ROOT/'go.sum']
        save(root/'environment.json',dict(platform=platform.platform(),machine=platform.machine(),cpu_model=command('sysctl','-n','machdep.cpu.brand_string'),hardware_model=command('sysctl','-n','hw.model'),logical_cpus=command('sysctl','-n','hw.ncpu'),memory_bytes=command('sysctl','-n','hw.memsize'),go=command('go','version'),placement='client and server on same host over loopback',gomaxprocs_per_process=4,source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},thresholds={'p95_ms_max':50,'failed_fraction_max':.01,'missed_fraction_max':.001,'scheduling_p95_ms_max':5,'throughput_fraction_min':.98},window='30s',repeats=3,seed_per_repeat=[1,2,3]))
    if args.phase=='pilot':
        selected={}
        for scenario in PRIMARY:
            results=[]
            for rate in [10,50,100,500,1000,5000,15000]:
                r=execute(root,'pilot',scenario,rate,1,'2s');results.append(r)
                if rate>=500 and not r['passes_threshold']:break
            # Baseline 100 plus near/onset and beyond degradation. A pilot is
            # exploratory; only 30-second repeated runs determine final claims.
            onset=results[-1]['rate']
            near=max([r["rate"] for r in results if r["passes_threshold"]] or [50])
            high=min(30000,max(500,onset if not results[-1]["passes_threshold"] else onset*2))
            selected[scenario]=sorted(set([100,near,high]))
            if len(selected[scenario])<3:selected[scenario]=[50,100,high]
        save(root/'selected-rates.json',selected)
        print('SELECTED '+json.dumps(selected),flush=True)
    else:
        selected=json.loads((root/'selected-rates.json').read_text())
        tasks=[(s,r,rep) for s,rates in selected.items() for r in rates for rep in [1,2,3]]
        tasks += [(s,100,rep) for s in ['zlib_repeating','lz4_random','lz4_repeating'] for rep in [1,2,3]]
        random.Random(151).shuffle(tasks)
        if (root/'measurement-order.json').exists():
            prior=json.loads((root/'measurement-order.json').read_text())
            known={tuple(t) for t in prior}
            tasks=prior+[t for t in tasks if tuple(t) not in known]
        save(root/'measurement-order.json',tasks)
        for scenario,rate,repeat in tasks:execute(root,'measure',scenario,rate,repeat,'30s')
        print('ALL MEASUREMENTS COMPLETE',flush=True)

if __name__=='__main__':main()
