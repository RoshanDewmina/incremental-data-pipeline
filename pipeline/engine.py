import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

STATES = {'queued','running','retry_wait','succeeded','failed','cancelled'}
MAX_LINE = 16384


def canonical(event):
    return json.dumps(event, sort_keys=True, separators=(',', ':'), allow_nan=False)


def validate(event):
    if not isinstance(event, dict):
        raise ValueError('object_required')
    if type(event.get('schema_version')) is not int or event['schema_version'] != 1:
        raise ValueError('unsupported_schema')
    for key in ('event_id','job_id','tenant_id','source','event_type'):
        if not isinstance(event.get(key), str) or not 1 <= len(event[key]) <= 100:
            raise ValueError('invalid_'+key)
    if event['event_type'] != 'job.state_changed' or event['source'] != 'durable-workflows':
        raise ValueError('unsupported_event_type_or_source')
    if type(event.get('sequence')) is not int or event['sequence'] < 1:
        raise ValueError('invalid_sequence')
    timestamp = event.get('occurred_at')
    if not isinstance(timestamp, str): raise ValueError('invalid_timestamp')
    try: parsed = datetime.fromisoformat(timestamp.replace('Z','+00:00'))
    except ValueError: raise ValueError('invalid_timestamp') from None
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError('timestamp_must_be_utc')
    payload = event.get('payload')
    if not isinstance(payload, dict) or payload.get('state') not in STATES or payload.get('kind') != 'data_import':
        raise ValueError('invalid_payload')
    for field in ('attempt','record_count'):
        if type(payload.get(field)) is not int or not 0 <= payload[field] <= 1000000:
            raise ValueError('invalid_'+field)
    canonical(event)
    return event


class Pipeline:
    def __init__(self, path):
        self.path = str(path)
        with self.connect() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,sequence INTEGER NOT NULL,occurred_at TEXT NOT NULL,body TEXT NOT NULL,UNIQUE(tenant_id,job_id,sequence));
            CREATE TABLE IF NOT EXISTS job_latest(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,sequence INTEGER NOT NULL,state TEXT NOT NULL,attempt INTEGER NOT NULL,record_count INTEGER NOT NULL,occurred_at TEXT NOT NULL,event_id TEXT NOT NULL REFERENCES events(event_id),PRIMARY KEY(tenant_id,job_id));
            CREATE TABLE IF NOT EXISTS sources(source_hash TEXT PRIMARY KEY,line_count INTEGER NOT NULL,checkpoint INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS lineage(source_hash TEXT NOT NULL,line_number INTEGER NOT NULL,event_id TEXT NOT NULL,outcome TEXT NOT NULL,PRIMARY KEY(source_hash,line_number));
            CREATE TABLE IF NOT EXISTS quarantine(source_hash TEXT NOT NULL,line_number INTEGER NOT NULL,reason TEXT NOT NULL,raw_hash TEXT NOT NULL,preview TEXT NOT NULL,PRIMARY KEY(source_hash,line_number));
            CREATE INDEX IF NOT EXISTS events_time ON events(occurred_at);
            ''')

    def connect(self):
        db=sqlite3.connect(self.path, timeout=10)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA foreign_keys=ON')
        return db

    def _apply(self, db, raw, source_hash, number):
        try:
            if len(raw.encode()) > MAX_LINE: raise ValueError('line_too_large')
            event=validate(json.loads(raw))
            body=canonical(event)
            prior=db.execute('SELECT body FROM events WHERE event_id=?',(event['event_id'],)).fetchone()
            if prior:
                if prior['body'] != body: raise ValueError('event_id_conflict')
                outcome='duplicate'
            else:
                collision=db.execute('SELECT event_id FROM events WHERE tenant_id=? AND job_id=? AND sequence=?',(event['tenant_id'],event['job_id'],event['sequence'])).fetchone()
                if collision: raise ValueError('job_sequence_conflict')
                db.execute('INSERT INTO events VALUES (?,?,?,?,?,?)',(event['event_id'],event['tenant_id'],event['job_id'],event['sequence'],event['occurred_at'],body))
                p=event['payload']
                db.execute('''INSERT INTO job_latest VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,job_id) DO UPDATE SET sequence=excluded.sequence,state=excluded.state,attempt=excluded.attempt,record_count=excluded.record_count,occurred_at=excluded.occurred_at,event_id=excluded.event_id WHERE excluded.sequence > job_latest.sequence''',(event['tenant_id'],event['job_id'],event['sequence'],p['state'],p['attempt'],p['record_count'],event['occurred_at'],event['event_id']))
                outcome='accepted'
            db.execute('INSERT INTO lineage VALUES (?,?,?,?)',(source_hash,number,event['event_id'],outcome))
            return outcome
        except (ValueError, TypeError, OverflowError) as exc:
            reason=str(exc) if str(exc) in {'object_required','unsupported_schema','unsupported_event_type_or_source','invalid_sequence','invalid_timestamp','timestamp_must_be_utc','invalid_payload','invalid_attempt','invalid_record_count','event_id_conflict','job_sequence_conflict','line_too_large'} or str(exc).startswith('invalid_') else 'malformed_json_or_value'
            db.execute('INSERT INTO quarantine VALUES (?,?,?,?,?)',(source_hash,number,reason,hashlib.sha256(raw.encode()).hexdigest(),raw[:500]))
            return 'quarantined'

    def ingest(self, path, batch_size=100, stop_after_batches=None):
        if not 1 <= batch_size <= 1000: raise ValueError('batch_size must be1..1000')
        content=Path(path).read_bytes()
        if len(content) > 20_000_000: raise ValueError('source exceeds20MB demo limit')
        digest=hashlib.sha256(content).hexdigest()
        # JSON Lines uses literal LF framing; Unicode separators inside JSON strings are data.
        lines=content.decode('utf-8').split('\n')
        if lines[-1]=='':lines.pop()
        lines=[line.removesuffix('\r') for line in lines]
        stats={'accepted':0,'duplicate':0,'quarantined':0,'source_hash':digest,'line_count':len(lines)}
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO sources VALUES (?,?,0)',(digest,len(lines)))
        batches=0
        while True:
            with self.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                checkpoint=db.execute('SELECT checkpoint FROM sources WHERE source_hash=?',(digest,)).fetchone()[0]
                if checkpoint>=len(lines): break
                end=min(len(lines),checkpoint+batch_size)
                for offset in range(checkpoint,end):
                    result=self._apply(db,lines[offset],digest,offset+1)
                    stats[result]+=1
                db.execute('UPDATE sources SET checkpoint=? WHERE source_hash=?',(end,digest))
            batches+=1
            if stop_after_batches is not None and batches>=stop_after_batches: break
        with self.connect() as db:
            stats['checkpoint']=db.execute('SELECT checkpoint FROM sources WHERE source_hash=?',(digest,)).fetchone()[0]
        stats['complete']=stats['checkpoint']==len(lines)
        return stats

    def analytics(self):
        with self.connect() as db:
            groups=[dict(r) for r in db.execute('SELECT tenant_id,state,count(*) AS jobs,sum(record_count) AS records,sum(attempt) AS attempts FROM job_latest GROUP BY tenant_id,state ORDER BY tenant_id,state')]
            totals={table:db.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in ('events','job_latest','lineage','quarantine')}
            errors=[dict(r) for r in db.execute('SELECT reason,count(*) AS count FROM quarantine GROUP BY reason ORDER BY reason')]
            sources=[dict(r) for r in db.execute('SELECT source_hash,line_count,checkpoint FROM sources ORDER BY source_hash')]
        return {'groups':groups,'totals':totals,'errors':errors,'sources':sources,'meaning':'Current job state by highest sequence, never arrival order; original events retained'}

    def oracle(self):
        with self.connect() as db:
            events=[json.loads(r[0]) for r in db.execute('SELECT body FROM events')]
            actual=[dict(r) for r in db.execute('SELECT * FROM job_latest ORDER BY tenant_id,job_id')]
        latest={}
        for e in sorted(events,key=lambda e:(e['tenant_id'],e['job_id'],e['sequence'])):
            p=e['payload']
            latest[(e['tenant_id'],e['job_id'])]={'tenant_id':e['tenant_id'],'job_id':e['job_id'],'sequence':e['sequence'],'state':p['state'],'attempt':p['attempt'],'record_count':p['record_count'],'occurred_at':e['occurred_at'],'event_id':e['event_id']}
        expected=[latest[key] for key in sorted(latest)]
        return {'converged':actual==expected,'job_count':len(expected),'expected_sha256':hashlib.sha256(canonical(expected).encode()).hexdigest(),'actual_sha256':hashlib.sha256(canonical(actual).encode()).hexdigest(),'scope':'Full Python recomputation over immutable accepted events; conflicts quarantined with first-accepted identity retained'}

    def quality(self):
        with self.connect() as db:
            integrity=db.execute('PRAGMA quick_check').fetchone()[0]
            foreign_keys=[tuple(r) for r in db.execute('PRAGMA foreign_key_check')]
            source_counts=[]
            for row in db.execute('SELECT source_hash,checkpoint FROM sources ORDER BY source_hash'):
                lineage=db.execute('SELECT count(*) FROM lineage WHERE source_hash=?',(row['source_hash'],)).fetchone()[0]
                quarantined=db.execute('SELECT count(*) FROM quarantine WHERE source_hash=?',(row['source_hash'],)).fetchone()[0]
                source_counts.append({'source_hash':row['source_hash'],'checkpoint':row['checkpoint'],'accounted_lines':lineage+quarantined,'passed':row['checkpoint']==lineage+quarantined})
            gaps=[dict(r) for r in db.execute('SELECT tenant_id,job_id,count(*) AS observed,min(sequence) AS minimum,max(sequence) AS maximum FROM events GROUP BY tenant_id,job_id HAVING count(*) != max(sequence)')]
        projected=self.oracle()
        return {'passed':integrity=='ok' and not foreign_keys and all(s['passed'] for s in source_counts) and projected['converged'],'integrity':integrity,'foreign_key_violations':foreign_keys,'source_accounting':source_counts,'projection_converged':projected['converged'],'sequence_gap_warnings':gaps,'gap_note':'Gaps may be temporarily valid for out-of-order/partial snapshots; they are visible warnings, not silently complete histories'}

    def rebuild(self):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('DELETE FROM job_latest')
            db.execute('''INSERT INTO job_latest SELECT tenant_id,job_id,sequence,json_extract(body,'$.payload.state'),json_extract(body,'$.payload.attempt'),json_extract(body,'$.payload.record_count'),occurred_at,event_id FROM (SELECT *,row_number() OVER(PARTITION BY tenant_id,job_id ORDER BY sequence DESC) AS rank FROM events) WHERE rank=1''')
        return self.oracle()
