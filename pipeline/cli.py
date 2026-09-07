import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import datetime,timezone
from pathlib import Path

from .engine import Pipeline
from .generator import generate

ROOT=Path(__file__).resolve().parents[1]


def benchmark():
    with tempfile.TemporaryDirectory() as directory:
        directory=Path(directory)
        source=directory/'events.ndjson'
        conditions=generate(source,2000,17)
        db=Pipeline(directory/'analytics.sqlite')
        started=time.perf_counter();partial=db.ingest(source,73,stop_after_batches=3);interrupted_ms=(time.perf_counter()-started)*1000
        resumed=Pipeline(directory/'analytics.sqlite')
        started=time.perf_counter();result=resumed.ingest(source,73);ingest_seconds=time.perf_counter()-started
        before=resumed.oracle()
        started=time.perf_counter();replay=resumed.ingest(source);replay_ms=(time.perf_counter()-started)*1000
        appended=directory/'appended.ndjson'
        appended.write_text(source.read_text()+'\n')
        backfill_started=time.perf_counter();backfill=resumed.ingest(appended);backfill_seconds=time.perf_counter()-backfill_started
        started=time.perf_counter();rebuilt=resumed.rebuild();rebuild_ms=(time.perf_counter()-started)*1000
        query_times=[]
        for _ in range(20):
            start=time.perf_counter();analytics=resumed.analytics();query_times.append((time.perf_counter()-start)*1000)
        assert before['converged'] and rebuilt['converged'] and before['actual_sha256']==rebuilt['actual_sha256']
        assert analytics['totals']['job_latest']==2000 and analytics['totals']['events']==6000
        assert replay['accepted']==0 and replay['duplicate']==0
        receipt={'schema_version':1,'source_revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'dirty_tree':bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip()),'timestamp':datetime.now(timezone.utc).isoformat(),'command':'make benchmark','exit_status':0,'environment':{'os':platform.platform(),'architecture':platform.machine(),'python':sys.version,'cpu_count':os.cpu_count()},'inputs':conditions,'results':{'partial_checkpoint':partial['checkpoint'],'partial_elapsed_ms':interrupted_ms,'resume':result,'resume_events_per_second':(result['accepted']+result['duplicate']+result['quarantined'])/ingest_seconds,'replay_ms':replay_ms,'backfill_seconds':backfill_seconds,'backfill':backfill,'rebuild_ms':rebuild_ms,'median_query_ms':statistics.median(query_times),'oracle':rebuilt,'analytics':analytics},'limitations':['Synthetic local workload; same source order for identity conflicts','Resume is tested after committed batch boundary; pytest also kills a child mid-run','20MB source cap, SQLite single host, no streaming broker','Rates reflect this host and cache conditions; not production throughput']}
        (ROOT/'evidence/benchmark.json').write_text(json.dumps(receipt,indent=2)+'\n')
        print(json.dumps({k:v for k,v in receipt['results'].items() if k!='analytics'},indent=2))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--db',default=os.environ.get('PIPELINE_DB','analytics.sqlite'))
    sub=parser.add_subparsers(dest='action',required=True)
    gen=sub.add_parser('generate');gen.add_argument('path');gen.add_argument('--jobs',type=int,default=500);gen.add_argument('--seed',type=int,default=17)
    ingest=sub.add_parser('ingest');ingest.add_argument('path');ingest.add_argument('--batch-size',type=int,default=100)
    for name in ('analytics','verify','quality','rebuild','benchmark'):sub.add_parser(name)
    args=parser.parse_args()
    if args.action=='generate': result=generate(args.path,args.jobs,args.seed)
    elif args.action=='benchmark':benchmark();return
    else:
        db=Pipeline(args.db)
        if args.action=='ingest':result=db.ingest(args.path,args.batch_size)
        elif args.action=='analytics':result=db.analytics()
        elif args.action=='verify':result=db.oracle()
        elif args.action=='quality':result=db.quality()
        else:result=db.rebuild()
    print(json.dumps(result,indent=2))
    if args.action=='quality' and not result['passed']:raise SystemExit(1)
    if args.action in ('verify','rebuild') and not result['converged']:raise SystemExit(1)


if __name__=='__main__':main()
