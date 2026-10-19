"""ADR0365 shared pure query builder and historical diagnostic assessment."""
import math
from uuid import UUID

from cce_hourly_window import QUERY


def proposed_program(events,start,rate):
    if (type(rate) is not int or rate not in (180,200) or len(events)!=rate
            or len(set(events))!=rate or any(str(UUID(v))!=v for v in events)
            or type(start) not in (int,float) or not math.isfinite(start) or start<=0):
        raise ValueError('Exact bounded cohort, supported rate and finite start required')
    expected,target=rate*300,rate*300-600
    query=QUERY.replace('3598','298').replace('3600','300').replace('300000',str(target)).replace('302400',str(expected))
    old=f"{target}<=row[4]<=row[0]<={expected}"
    new=f"{target}<=row[0]<={expected} and {rate}*296<=row[4]<=row[0]"
    if query.count(old)!=1:raise ValueError('Exact unchanged issuance predicate required')
    query=query.replace(old,new)
    query=query.replace("print(json.dumps(result))", "result['guarded_window_seconds']=296\nresult['full_window_tickets_per_second']=row[0]/300\nresult['guarded_window_tickets_per_second']=row[4]/296\nprint(json.dumps(result))")
    return 'events='+repr(events)+'\nstart='+repr(start)+'\n'+query


def assessment(full,guarded,rate):
    if (type(rate) is not int or rate not in (180,200)
            or type(full) is not int or type(guarded) is not int
            or not 0<=guarded<=full<=rate*300):
        raise ValueError('Exact bounded integer counts required')
    target=rate*300-600
    return {'proposal_only':True,'offered_rate':rate,'full_window_seconds':300,
            'guarded_window_seconds':296,'full_count':full,'guarded_count':guarded,
            'minimum_full_count_unchanged':target,'minimum_guarded_count':rate*296,
            'full_window_tickets_per_second':full/300,'guarded_window_tickets_per_second':guarded/296,
            'original_count_predicate':guarded>=target,
            'proposed_count_predicate':full>=target and guarded>=rate*296,
            'load_authorized':False,'historical_status_changed':False}
