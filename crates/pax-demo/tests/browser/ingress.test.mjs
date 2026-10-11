import test from 'node:test';
import assert from 'node:assert/strict';
import {ingressClockModels,ingressSummary,ingressParts,ingressContributions,preferredIngressSource} from '../../src/presentation/assets/ingress.mjs';

function observation(index,before=1000,decode=100) {
  const trace={id:`7:${BigInt(index)+9007199254740992n}`,generation:'7',market:'1',sequence:String(index),population:'event',received_ns:10000+index,
    policy_revision:1,queue_ns:20,processing_ns:decode+50,decode_ns:decode,preflight_ns:20,mutation_ns:10,validation_ns:10,other_ns:10};
  const receive={id:trace.id,received_ns:trace.received_ns,revision:3,complete_policy_window:true,socket:{first:'1',last:'1'},observed_last_read_to_message_ns:100,
    software:{source:'kernel-software/realtime',status:'read-batch proxy',batch_to_message_ns:before,uncertainty_ns:5},
    hardware:{source:'unavailable',batch_to_message_ns:null,uncertainty_ns:null}};
  return {trace,receive};
}
function view(...observations) {
  return {mode:'live',markets:[{id:'1',generation:'7',status:'live'}],latency:{stale:false,
    traces:observations.map(o=>o.trace),receive:observations.map(o=>o.receive)}};
}
test('automatic source selection requires qualified NIC samples and preserves manual choice',()=>{
  const o=observation(0); const v=view(o);
  assert.equal(preferredIngressSource(v),'software');
  o.receive.hardware={source:'nic-hardware/phc',batch_to_message_ns:5000,uncertainty_ns:100};
  assert.equal(preferredIngressSource(v),'hardware');
  assert.equal(preferredIngressSource(v,'software'),'software');
  o.receive.hardware.uncertainty_ns=null;
  assert.equal(preferredIngressSource(v),'software');
});
test('ranks totals first and retains actual event components, exact identities and percentages',()=>{
  const a=observation(0,1000,800),b=observation(1,2000,10),c=observation(2,3000,100);
  const summary=ingressSummary(view(c,a,b,a));
  assert.equal(summary.samples,3);assert.equal(summary.eligible,3);
  assert.equal(summary.p50.trace.id,b.trace.id);assert.equal(summary.p50.total_ns,2080);
  assert.equal(summary.p99.trace.id,c.trace.id);assert.equal(summary.p99.total_ns,3170);
  const rows=ingressContributions(summary);
  assert.equal(rows.find(r=>r.label==='Decode').p99_ns,100); // Not the 800 ns independent tail.
  for (const percentile of ['p50','p99']) {
    assert.equal(rows.reduce((sum,r)=>sum+r[`${percentile}_ns`],0),summary[percentile].total_ns);
    assert.ok(Math.abs(rows.reduce((sum,r)=>sum+r[`${percentile}_percent`],0)-100)<1e-10);
  }
  assert.deepEqual(summary.receive_revisions,[3]);assert.deepEqual(summary.processing_revisions,[1]);
});
test('uses nearest-rank quantiles at the boundary and never conflates hardware/software populations',()=>{
  const observations=Array.from({length:100},(_,i)=>observation(i,1000+i));
  observations[0].receive.hardware={source:'nic-hardware/phc',batch_to_message_ns:5000,uncertainty_ns:100};
  const v=view(...observations);const s=ingressSummary(v);
  assert.equal(s.p50.trace.id,observations[49].trace.id);assert.equal(s.p99.trace.id,observations[98].trace.id);
  const h=ingressSummary(v,'hardware');assert.equal(h.samples,1);assert.equal(h.p50.total_ns,5170);
  assert.throws(()=>ingressSummary(v,'origin'),/Unknown/);
});
test('replay, unsynchronized books, wrong generation and noncommits cannot fabricate ingress latency',()=>{
  for (const change of [v=>v.mode='replay',v=>v.mode='fixture',v=>v.markets[0].status='awaiting snapshot',v=>v.markets[0].generation='8',
    ...['rejected','buffered','duplicate','control','snapshot'].map(population=>v=>v.latency.traces[0].population=population),
    v=>v.latency.traces[0].id='8:1']) {
    const v=view(observation(0));change(v);assert.equal(ingressSummary(v).samples,0);
  }
});
test('requires equal receipt markers, valid clock arithmetic and complete policy windows',()=>{
  for (const change of [r=>r.id='7:999',r=>r.received_ns++,r=>r.complete_policy_window=false,
    r=>r.software.source='unknown-clock',
    r=>r.software.batch_to_message_ns=null,r=>r.software.batch_to_message_ns=-1,
    r=>r.software.uncertainty_ns=null,r=>r.software.batch_to_message_ns=Number.MAX_SAFE_INTEGER]) {
    const o=observation(0);change(o.receive);assert.equal(ingressSummary(view(o)).samples,0);
  }
  const o=observation(0);o.trace.queue_ns=null;assert.equal(ingressSummary(view(o)).samples,0);
});
test('partial diagnostics retain the enclosing measured span without biasing the headline population',()=>{
  const a=observation(0),b=observation(1,3000);a.trace.decode_ns=null;b.receive.observed_last_read_to_message_ns=null;
  const summary=ingressSummary(view(a,b));assert.equal(summary.samples,2);
  const rows=ingressContributions(summary);
  assert.deepEqual(rows.map(r=>r.label),['Ingress → complete message','Message dispatch / preparation','Decode + book processing']);
  assert.equal(rows.reduce((sum,r)=>sum+r.p99_ns,0),summary.p99.total_ns);
});
test('never partitions using unrelated latest_read or adds active receive work',()=>{
  const o=observation(0);o.receive.observed_last_read_to_message_ns=2000;
  o.receive.latest_read={ended_to_message_ns:5};o.receive.work={tls_ns:1e9};
  const sample=ingressSummary(view(o)).p50;const parts=ingressParts(sample);
  assert.equal(parts[0].label,'Ingress → complete message');
  assert.equal(parts.reduce((sum,p)=>sum+p.ns,0),1170);
});
test('zero totals have unavailable percentages, and stale windows remain marked',()=>{
  const o=observation(0,0,0);for(const k of ['queue_ns','processing_ns','preflight_ns','mutation_ns','validation_ns','other_ns'])o.trace[k]=0;
  const v=view(o);v.latency.stale=true;const summary=ingressSummary(v);
  assert.equal(summary.stale,true);assert.equal(summary.p50.total_ns,0);
  assert.ok(ingressContributions(summary).every(r=>r.p50_percent===null));
});
test('first-window populations preserve their earlier boundary and separate completion samples',()=>{
  const o=observation(0);
  o.receive.socket={first:'9007199254740993',last:'9007199254740994'};
  o.receive.first_observed_read={batch:'9007199254740993',ended_to_message_ns:300,
    software:{source:'kernel-software/realtime',batch_to_message_ns:4000,uncertainty_ns:8},
    hardware:{source:'nic-hardware/phc',batch_to_message_ns:4200,uncertainty_ns:30}};
  const v=view(o),first=ingressSummary(v,'first-software');
  assert.equal(first.p50.total_ns,4170);
  assert.equal(first.boundary_label,'First observed read-window proxy');
  assert.equal(ingressSummary(v,'software').p50.total_ns,1170);
  assert.equal(preferredIngressSource(v),'first-hardware');
  const parts=ingressParts(first.p50,'first-software');
  assert.deepEqual(parts.slice(0,3).map(p=>p.ns),[3700,200,100]);
  assert.equal(parts.reduce((sum,p)=>sum+p.ns,0),first.p50.total_ns);
  assert.ok(Math.abs(ingressContributions(first).reduce((sum,p)=>sum+p.p50_percent,0)-100)<1e-10);
  assert.equal(preferredIngressSource(v,'hardware'),'hardware');
});
function readWindow(index,first=600,last=100) {
  const o=observation(index);
  o.receive.socket={first:'9007199254740993',last:'9007199254740994'};
  o.receive.observed_last_read_to_message_ns=last;
  o.receive.first_observed_read={batch:o.receive.socket.first,ended_to_message_ns:first,
    software:{source:'kernel-software/realtime',batch_to_message_ns:1000+index*1000,uncertainty_ns:5}};
  return o;
}
test('first-window chronology preserves total-ranked events, matched percentages and shared/buffered flags',()=>{
  const a=readWindow(0,600,100),b=readWindow(1,700,200);
  a.receive.buffered=b.receive.shared=true;
  const summary=ingressSummary(view(b,a),'first-software'),rows=ingressContributions(summary);
  assert.equal(summary.p50.trace.id,a.trace.id);assert.equal(summary.p99.trace.id,b.trace.id);
  assert.deepEqual(rows.slice(0,3).map(r=>[r.label,r.p50_ns,r.p99_ns]),[
    ['Ingress → first observed read end',400,1300],
    ['First → last observed read end',500,500],
    ['Last read end → complete message',100,200]]);
  for(const p of ['p50','p99']) {
    assert.equal(rows.reduce((sum,r)=>sum+r[`${p}_ns`],0),summary[p].total_ns);
    assert.ok(Math.abs(rows.reduce((sum,r)=>sum+r[`${p}_percent`],0)-100)<1e-10);
  }
  assert.equal(summary.p50.receive.buffered,true);assert.equal(summary.p99.receive.shared,true);
  assert.ok(ingressParts(summary.p50,'first-software')[1].note.includes('Not isolated network wait'));
  const completion=ingressSummary(view(a,b),'software');
  assert.equal(ingressParts(completion.p50,'software')[1].label,'Read end → complete message');
});
test('partial or inconsistent last-read detail falls back in both table events without changing headlines',()=>{
  for(const change of [r=>delete r.observed_last_read_to_message_ns,
    r=>r.observed_last_read_to_message_ns=-1,r=>r.observed_last_read_to_message_ns=601,
    r=>r.observed_last_read_to_message_ns=Number.MAX_SAFE_INTEGER+1,
    r=>r.socket.last=r.socket.first]) {
    const a=readWindow(0),b=readWindow(1);change(a.receive);
    const s=ingressSummary(view(a,b),'first-software');
    assert.equal(s.samples,2);assert.equal(s.p50.total_ns,1170);assert.equal(s.p99.total_ns,2170);
    const rows=ingressContributions(s);
    assert.equal(rows[1].label,'First read end → complete message');
    assert.equal(rows.reduce((sum,r)=>sum+r.p99_ns,0),2170);
    assert.equal(ingressParts(s.p99,'first-software')[1].label,'First → last observed read end');
  }
});
test('single-read windows allow only identical completion times and preserve zero spans',()=>{
  const o=readWindow(0,100,100);o.receive.socket.last=o.receive.socket.first;
  const s=ingressSummary(view(o),'first-software');
  assert.deepEqual(ingressParts(s.p50,'first-software').slice(0,3).map(r=>r.ns),[900,0,100]);
  assert.equal(ingressContributions(s)[1].p50_percent,0);
  o.receive.observed_last_read_to_message_ns=99;
  assert.equal(ingressParts(ingressSummary(view(o),'first-software').p50,'first-software')[1].label,'First read end → complete message');
});
test('first-window selection never replaces missing or mismatched metadata with completion timing',()=>{
  const o=observation(0);
  o.receive.hardware={source:'nic-hardware/phc',batch_to_message_ns:2000,uncertainty_ns:10};
  for(const first of [undefined,{batch:'2',software:o.receive.software},
    {batch:'1',software:{source:'kernel-software/realtime',batch_to_message_ns:null,uncertainty_ns:5}}]) {
    o.receive.first_observed_read=first;
    assert.equal(ingressSummary(view(o),'first-software').samples,0);
    assert.equal(preferredIngressSource(view(o)),'hardware');
  }
  o.receive.first_observed_read={batch:'1',ended_to_message_ns:100,software:o.receive.software};
  assert.equal(preferredIngressSource(view(o)),'first-software');
  delete o.receive.socket;
  assert.equal(ingressSummary(view(o),'first-software').samples,0);
});

test('clock descriptions use matched first-read models and never a completion model',()=>{
  const o=observation(0);
  const model=epoch=>({generation:'17',epoch,method:'user_bracket',age_ns:1000000,bracket_uncertainty_ns:100,assumed_drift_ppm:100,mapping_deferred:false,rate_validated:false});
  o.receive.hardware={source:'nic-hardware/phc',batch_to_message_ns:1000,uncertainty_ns:200,qualification:model(99)};
  o.receive.socket={first:'10',last:'11'};
  o.receive.first_observed_read={batch:'10',ended_to_message_ns:300,hardware:{source:'nic-hardware/phc',batch_to_message_ns:4000,uncertainty_ns:200,qualification:model(7)}};
  const first=ingressClockModels(ingressSummary(view(o),'first-hardware'));
  assert.ok(first.every(row=>row.text.includes('epoch 7 / connection 17')&&!row.text.includes('epoch 99')));
  assert.ok(first.every(row=>row.text.includes('not validated')&&row.text.includes('1.00 ms')));
  const completion=ingressClockModels(ingressSummary(view(o),'hardware'));
  assert.ok(completion.every(row=>row.text.includes('epoch 99')));
  delete o.receive.first_observed_read.hardware.qualification;
  assert.ok(ingressClockModels(ingressSummary(view(o),'first-hardware')).every(row=>row.text.includes('details unavailable')&&!row.text.includes('epoch 99')));
  assert.ok(ingressClockModels(ingressSummary(view())).every(row=>row.text.includes('no qualified')));
});
