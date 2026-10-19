"""End-to-end lesson scenarios. Requires fresh initialized single AND cluster labs.

Creates tutorial records. Use only in this learning environment.
"""
from pathlib import Path
import subprocess
import time
import uuid
import re

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'검증'/'결과';OUT.mkdir(parents=True,exist_ok=True)
RUN='qa-'+uuid.uuid4().hex[:8]
COMPOSE=['docker','compose','-f',str(ROOT/'실습'/'compose.yaml'),'-p','kafka-field-guide']
summary=[]

def invoke(name,args,expected=0,timeout=120):
    command=['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'실습'/'study.ps1')]+args
    result=subprocess.run(command,cwd=ROOT,capture_output=True,encoding='utf-8',errors='replace',timeout=timeout)
    text=result.stdout+result.stderr
    (OUT/(name+'.log')).write_text(text,encoding='utf-8')
    if expected==0 and result.returncode!=0:raise AssertionError(name+' failed:\n'+text[-5000:])
    if expected!=0 and not (result.returncode!=0 and 'CRASH' in text and 'exit 17' in text):raise AssertionError(name+' did not reproduce expected crash:\n'+text)
    return text

def check(condition,label):
    if not condition:raise AssertionError(label)
    print('PASS '+label,flush=True);summary.append('PASS '+label)

def compose(*args):
    result=subprocess.run(COMPOSE+list(args),cwd=ROOT,capture_output=True,encoding='utf-8',errors='replace',timeout=120)
    if result.returncode:raise AssertionError(result.stderr)
    return result.stdout

def main():
    sent=invoke('scenario-producer',['run','producer','6','orders',RUN])
    check(sent.count('SENT event=')==6,'lesson producer publishes six records')
    consumed=invoke('scenario-consumer',['run','consumer',RUN+'-read','0','orders'])
    check(all(RUN+'-'+str(i) in consumed for i in range(6)),'lesson consumer reads all generated eventIds')
    crash=invoke('scenario-commit-crash',['run','commit','crash-after',RUN+'-commit','1'],expected=17)
    match=re.search(r'PROCESSED .*p=(\d+) offset=(\d+)',crash)
    normal=invoke('scenario-commit-restart',['run','commit','after',RUN+'-commit','0'])
    check(bool(match) and f'p={match[1]} offset={match[2]} ' in normal,'uncommitted processing is replayed after crash')
    invoke('scenario-dedup-crash',['run','dedup',RUN+'-dedup','1','crash-after-db'],expected=17)
    dedup=invoke('scenario-dedup-restart',['run','dedup',RUN+'-dedup','0','normal'])
    check('DUPLICATE_SKIPPED' in dedup,'DB-committed event is skipped on consumer recovery')
    invoke('scenario-poison',['run','poison'])
    retry=invoke('scenario-retry',['run','retry',RUN+'-retry','3'])
    check('attempt=2' in retry and retry.count('DLT p=')==2,'retry succeeds on second attempt; two permanent errors reach DLT')
    invoke('scenario-outbox-create',['run','outbox-create','1',RUN+'-outbox'])
    crash=invoke('scenario-outbox-crash',['run','outbox-relay','crash-after-send','1'],expected=17)
    relay=invoke('scenario-outbox-restart',['run','outbox-relay','normal','10'])
    event=re.search(r'RELAY_ACK (\S+)',crash)
    check(bool(event) and 'RELAY_ACK '+event[1] in relay,'outbox ack-before-mark crash resends same eventId')
    invoke('scenario-transaction-pipeline',['run','tx-process',RUN+'-tx','6'])
    visible=invoke('scenario-transaction-output',['run','consumer',RUN+'-tx-view','0','orders.tx','0','read_committed'])
    check('RECEIVED' in visible,'Kafka-to-Kafka transaction produces committed output')
    streams=invoke('scenario-streams',['run','streams','35',RUN+'-counts-single'])
    check('RUNNING' in streams and 'ERROR' not in streams,'Streams topology reaches RUNNING and shuts down cleanly')
    counts=invoke('scenario-streams-counts',['run','consumer',RUN+'-counts-view','0','orders.counts'])
    check('value=' in counts,'Streams emits serialized counts')
    before=compose('--profile','cluster','exec','-T','kafka2','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka2:9092','--describe','--topic','orders')
    (OUT/'scenario-failover-before.log').write_text(before,encoding='utf-8')
    invoke('scenario-stop-broker',['stop','kafka1'])
    try:
        # Wait by polling metadata; allow failure detector/election time.
        deadline=time.monotonic()+70
        after=''
        while time.monotonic()<deadline:
            after=compose('--profile','cluster','exec','-T','kafka2','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka2:9092','--describe','--topic','orders')
            if not re.search(r'Leader:\s*(?:1|-1)\b',after):break
            time.sleep(2)
        (OUT/'scenario-failover-during.log').write_text(after,encoding='utf-8')
        sent=invoke('scenario-failover-producer',['run','-Cluster','producer','6','orders',RUN+'-failover'])
        check(sent.count('SENT event=')==6,'three-node cluster keeps writing with one broker stopped')
    finally:
        invoke('scenario-recover-broker',['start','kafka1'])
    (OUT/'scenarios-summary.log').write_text('\n'.join(summary)+'\n',encoding='utf-8')
    print('ALL_SCENARIOS_PASSED '+RUN,flush=True)

if __name__=='__main__':main()
