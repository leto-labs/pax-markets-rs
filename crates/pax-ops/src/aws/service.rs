//! Read-only release/recording status and an explicit demo restart with feed health.
use super::Api;
use crate::{config::Scope, error::Error};
use serde_json::Value;

const STATUS: &str = r#"python3 - <<'PY'
import hashlib,itertools,json,pathlib,subprocess,urllib.request
root=pathlib.Path('/opt/pax/releases')
def release(name):
    link=pathlib.Path('/opt/pax')/name
    if not link.is_symlink():return {'available':False,'reason':'no managed release'}
    target=link.resolve()
    if not target.is_relative_to(root):return {'available':False,'reason':'unmanaged release path'}
    result={'id':target.name}
    for name in ('build.json','effective-capabilities.json'):
        path=target/name
        if path.is_file() and path.stat().st_size<=1024*1024:result[name]=json.loads(path.read_text())
    unit=target/'pax-demo.service'
    if unit.is_file():result['unit_sha256']=hashlib.sha256(unit.read_bytes()).hexdigest()
    return result
p=subprocess.run(['systemctl','show','pax-demo.service','-p','ActiveState,SubState,ExecMainStatus,NRestarts,MainPID,FragmentPath,CPUUsageNSec,MemoryCurrent'],capture_output=True,text=True,timeout=10)
service=dict(line.split('=',1) for line in p.stdout.splitlines() if '=' in line)
try:
    state=json.load(urllib.request.urlopen('http://127.0.0.1/api/state',timeout=3))['state']
    view=state['view']
    health={'available':True,'connected':state['transport_connected'] and view['connected'],'books':[{'id':b['id'],'status':b['status'],'sequence':b['sequence']} for b in view['markets']]}
except (OSError,ValueError,KeyError) as error:health={'available':False,'reason':type(error).__name__}
recordings=[]
entries=list(itertools.islice(pathlib.Path('/var/lib/pax/captures').iterdir(),34))
for directory in entries[:33]:
    if not directory.is_dir() or directory.is_symlink() or directory.name=='.pending':continue
    manifest=directory/'manifest.json'
    try:
        if manifest.stat().st_size>1024*1024:raise ValueError('manifest bound')
        data=json.loads(manifest.read_text())
        recordings.append({'name':directory.name,'complete':data['complete'],'bytes':data['bytes'],'records':data['records'],'digest':data['digest'],'stop_reason':data['stop_reason']})
    except (OSError,ValueError,KeyError):recordings.append({'name':directory.name,'complete':False,'reason':'missing or invalid manifest'})
    if len(recordings)>=32:break
jobs=[]
job_paths=list(itertools.islice(pathlib.Path('/var/lib/pax/capture-ops').glob('*.json'),33))
for path in job_paths[:32]:
    try:
        if path.stat().st_size>1024*1024:raise ValueError('job status bound')
        data=json.loads(path.read_text());jobs.append({'id':path.stem,'status':data['status'],'segments':len(data['segments']),'error':data.get('error')})
    except (OSError,ValueError,KeyError):jobs.append({'id':path.stem,'status':'unavailable'})
print(json.dumps({'schema':1,'current':release('current'),'previous':release('previous'),'service':service,'health':health,'recordings':recordings,'jobs':jobs,'inventory_limit':32,'inventory_truncated':len(entries)>33 or len(recordings)==32 or len(job_paths)>32}))
PY
"#;
/// Inspect non-secret release identity, service health and complete/incomplete recordings.
pub fn status(api: &mut impl Api, scope: &Scope, instance: &str) -> Result<Value, Error> {
    let result = super::release::command(api, scope, instance, STATUS, 30)?;
    Ok(serde_json::from_str(
        result["StandardOutputContent"]
            .as_str()
            .ok_or(Error::Missing("service status stdout"))?,
    )?)
}
/// Restart only the demo, preserving all capture directories and jobs. Fail until
/// fresh books synchronize; a successful unit start alone is insufficient.
pub fn restart(api: &mut impl Api, scope: &Scope, instance: &str) -> Result<Value, Error> {
    let script = r#"set -eu
install -d -o root -g root -m 0700 /var/lib/pax-ops
exec 9>/var/lib/pax-ops/operator.lock
flock -w 30 9
python3 - <<'PY'
import json,pathlib,shlex,subprocess,time,urllib.request
release=pathlib.Path('/opt/pax/current').resolve(strict=True)
assert release.is_relative_to('/opt/pax/releases'), 'unmanaged release'
command=next(line.removeprefix('ExecStart=') for line in (release/'pax-demo.service').read_text().splitlines() if line.startswith('ExecStart='))
argv=shlex.split(command)
expected=set(argv[argv.index('--markets')+1].split(','))
assert expected, 'no configured books available for restart health'
subprocess.run(['systemctl','restart','pax-demo.service'],check=True,timeout=30)
for attempt in range(120):
    try:
        state=json.load(urllib.request.urlopen('http://127.0.0.1/api/state',timeout=2))['state'];view=state['view'];books=view['markets']
        if state['transport_connected'] and view['connected'] and expected=={b['id'] for b in books} and all(b['status']=='live' and b['sequence'] is not None for b in books):break
    except (OSError,ValueError,KeyError):pass
    time.sleep(.5)
else:raise SystemExit('restarted service did not synchronize')
PY
"#;
    super::release::command(api, scope, instance, script, 120)?;
    status(api, scope, instance)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn release_status_rejects_an_unowned_host_before_ssm() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut fake = super::super::fake::Fake::new([
            Ok(serde_json::json!({"Account":"123456789012"})),
            Ok(
                serde_json::json!({"Reservations":[{"Instances":[{"InstanceId":"i-test","Tags":[]}]}]}),
            ),
        ]);
        assert!(status(&mut fake, &scope, "i-test").is_err());
        assert!(fake.calls.iter().all(|(service, _, _)| service != "ssm"));
    }
}
