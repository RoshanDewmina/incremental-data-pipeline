import concurrent.futures
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pipeline.api import create_app
from pipeline.engine import Pipeline,canonical
from pipeline.generator import generate


def fixture(tmp_path,jobs=20):
    source=tmp_path/'events.ndjson';generate(source,jobs,17)
    return Pipeline(tmp_path/'db.sqlite'),source


def test_duplicate_late_malformed_additive_fields_and_oracle(tmp_path):
    db,source=fixture(tmp_path)
    r=db.ingest(source,7)
    assert r['accepted']==60 and r['duplicate']==2 and r['quarantined']==3
    assert db.analytics()['totals']['job_latest']==20
    assert db.oracle()['converged']
    assert all(g['state'] in ('failed','succeeded') for g in db.analytics()['groups'])
    before=db.oracle()['actual_sha256']
    assert db.ingest(source)['accepted']==0
    assert db.rebuild()['actual_sha256']==before


def test_checkpoint_resume_and_new_content_backfill(tmp_path):
    db,source=fixture(tmp_path)
    partial=db.ingest(source,7,1)
    assert partial['checkpoint']==7 and not partial['complete']
    resumed=Pipeline(db.path);r=resumed.ingest(source,7)
    assert r['complete'] and resumed.oracle()['converged']
    source.write_text(source.read_text()+'\n')
    backfill=resumed.ingest(source)
    assert backfill['accepted']==0 and backfill['duplicate']==62 and backfill['quarantined']==4


def test_competing_ingestion_checkpoints_claim_once(tmp_path):
    db,source=fixture(tmp_path,100)
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        results=list(pool.map(lambda _:Pipeline(db.path).ingest(source,3),range(4)))
    assert sum(r['accepted'] for r in results)==300
    assert db.analytics()['totals']['lineage']==310
    assert db.oracle()['converged']


def test_conflicting_event_and_sequence_quarantined(tmp_path):
    db,source=fixture(tmp_path,1)
    event=json.loads(source.read_text().splitlines()[0])
    source.write_text(json.dumps(event)+'\n')
    db.ingest(source)
    changed={**event,'payload':{**event['payload'],'record_count':99}}
    collision={**event,'event_id':'other'}
    other=tmp_path/'conflicts.ndjson';other.write_text(json.dumps(changed)+'\n'+json.dumps(collision))
    r=db.ingest(other)
    assert r['quarantined']==2
    assert {e['reason'] for e in db.analytics()['errors']}=={'event_id_conflict','job_sequence_conflict'}
    assert db.analytics()['totals']['events']==1


@pytest.mark.parametrize('change',[{'sequence':True},{'sequence':0},{'schema_version':True},{'schema_version':2},{'occurred_at':'2026-01-01'},{'occurred_at':'2026-01-01T00:00:00+02:00'},{'payload':{'state':'succeeded','kind':'data_import','attempt':True,'record_count':2}}])
def test_schema_rejects_invalid_values(tmp_path,change):
    db,source=fixture(tmp_path,1)
    e=json.loads(source.read_text().splitlines()[0]);source.write_text(json.dumps({**e,**change}))
    assert db.ingest(source)['quarantined']==1
    assert db.analytics()['totals']['events']==0


def test_killed_child_resumes_without_loss(tmp_path):
    db,source=fixture(tmp_path,1500)
    proc=subprocess.Popen([sys.executable,'-m','pipeline.cli','--db',db.path,'ingest',str(source),'--batch-size','1'],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,cwd=Path(__file__).resolve().parents[1])
    seen=False
    try:
        for _ in range(500):
            with db.connect() as con:
                checkpoint=con.execute('SELECT checkpoint FROM sources').fetchone()
            if checkpoint and checkpoint[0]>5:seen=True;break
            if proc.poll() is not None:break
            time.sleep(.005)
        assert seen and proc.poll() is None
        proc.kill();proc.wait(timeout=5)
    finally:
        if proc.poll() is None:proc.kill();proc.wait(timeout=5)
    assert db.ingest(source)['complete']
    assert db.oracle()['converged'] and db.analytics()['totals']['events']==4500


def test_readonly_http_dashboard_has_no_ingest_endpoint(tmp_path):
    db,source=fixture(tmp_path);db.ingest(source)
    with TestClient(create_app(db.path)) as c:
        assert c.get('/health').json()['service']=='incremental-data-pipeline'
        assert c.get('/analytics').json()['totals']['job_latest']==20
        assert c.post('/ingest',json={}).status_code==404
        assert 'Every event accounted for' in c.get('/').text


def test_quality_detects_projection_and_checkpoint_corruption(tmp_path):
    db,source=fixture(tmp_path);db.ingest(source)
    assert db.quality()['passed']
    with db.connect() as con:con.execute("UPDATE job_latest SET state='queued'")
    assert not db.quality()['passed']
    db.rebuild()
    assert db.quality()['passed']
    with db.connect() as con:con.execute('UPDATE sources SET checkpoint=checkpoint+1')
    assert not db.quality()['passed']


def test_valid_shuffles_converge_to_same_projection(tmp_path):
    import random
    _,source=fixture(tmp_path,30)
    lines=source.read_text().splitlines()[:-3]
    hashes=[]
    for seed in range(5):
        random.Random(seed).shuffle(lines)
        path=tmp_path/f'shuffle-{seed}.ndjson';path.write_text('\n'.join(lines))
        db=Pipeline(tmp_path/f'shuffle-{seed}.sqlite');db.ingest(path)
        assert db.quality()['passed']
        hashes.append(db.oracle()['actual_sha256'])
    assert len(set(hashes))==1


@pytest.mark.parametrize('separator',['\u2028','\u0085'])
@pytest.mark.parametrize('framing',['\n','\r\n'])
def test_unicode_inside_json_preserves_physical_lines_and_resume(tmp_path,separator,framing):
    db,source=fixture(tmp_path,1)
    events=[json.loads(line) for line in source.read_text().splitlines()[:3]]
    for event in events:event['note']='before'+separator+'after'
    source.write_bytes((framing.join(json.dumps(e,ensure_ascii=False) for e in events)+framing).encode())
    partial=db.ingest(source,1,1)
    assert partial['checkpoint']==1 and not partial['complete']
    result=Pipeline(db.path).ingest(source,1)
    assert result['complete'] and result['checkpoint']==3 and result['quarantined']==0
    assert db.analytics()['totals']['events']==3 and db.quality()['passed']
    with db.connect() as con:
        bodies=[json.loads(r[0]) for r in con.execute('SELECT body FROM events')]
        assert all(e['note']=='before'+separator+'after' for e in bodies)
    source.write_bytes(source.read_bytes()+framing.encode())
    backfill=db.ingest(source)
    assert backfill['duplicate']==3 and backfill['quarantined']==1
    assert backfill['checkpoint']==4 and db.quality()['passed']
