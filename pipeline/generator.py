import json
import random
import uuid
from datetime import datetime,timedelta,timezone
from pathlib import Path


def generate(path,jobs=500,seed=17):
    rng=random.Random(seed)
    events=[]
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    for index in range(jobs):
        tenant='synthetic-'+('alpha' if index%2==0 else 'beta')
        job=str(uuid.uuid5(uuid.NAMESPACE_URL,f'portfolio/job/{seed}/{index}'))
        count=rng.randint(1,100)
        for sequence,state in enumerate(['queued','running','succeeded' if index%7 else 'failed'],1):
            e={'schema_version':1,'event_id':str(uuid.uuid5(uuid.NAMESPACE_URL,f'{job}/{sequence}')),'source':'durable-workflows','event_type':'job.state_changed','occurred_at':(start+timedelta(seconds=index*10+sequence)).isoformat(),'job_id':job,'tenant_id':tenant,'sequence':sequence,'payload':{'state':state,'kind':'data_import','attempt':int(sequence>1),'record_count':count}}
            if index%11==0:e['compatible_extra']='v1 additive field'
            events.append(e)
    events+=events[:max(1,jobs//10)]
    rng.shuffle(events)
    lines=[json.dumps(e,sort_keys=True) for e in events]
    lines+=['{malformed',json.dumps({'schema_version':2}),json.dumps({**events[0],'payload':{**events[0]['payload'],'record_count':-1}})]
    Path(path).write_text('\n'.join(lines)+'\n')
    return {'jobs':jobs,'seed':seed,'lines':len(lines),'unique_events':jobs*3,'intentional_duplicates':max(1,jobs//10),'malformed':3,'out_of_order':True,'synthetic':True}
