import test from 'node:test';
import assert from 'node:assert/strict';
import {ingressSummary,ingressArrival,ingressArrivalContributions} from '../../src/presentation/assets/ingress.mjs';

// Epoch stamps deliberately exceed JS integer precision. The two associated
// arrivals are 709 us apart; the complete primary event remains 731 us.
function event(index=1) {
  const trace={id:`7:${index}`,generation:'7',market:'1',population:'event',received_ns:10000+index,
    queue_ns:800,processing_ns:400,decode_ns:200,preflight_ns:50,mutation_ns:50,validation_ns:50,other_ns:50};
  const stamp=(raw,before)=>({source:'nic-hardware/phc',raw_ns:raw,batch_to_message_ns:before,uncertainty_ns:4000});
  const receive={id:trace.id,received_ns:trace.received_ns,complete_policy_window:true,
    socket:{first:'9007199254740993',last:'9007199254740994'},observed_last_read_to_message_ns:3000,
    first_observed_read:{batch:'9007199254740993',ended_to_message_ns:715000,
      hardware:stamp('1791703394720710414',730000)},
    hardware:stamp('1791703394721419414',21000)};
  return {trace,receive};
}
function summary(...events) {
  return ingressSummary({mode:'live',markets:[{id:'1',generation:'7',status:'live'}],
    latency:{traces:events.map(e=>e.trace),receive:events.map(e=>e.receive)}},'first-hardware');
}
const arrival=s=>ingressArrival(s,'first-hardware');

test('arrival attribution keeps exact raw differences, primary ranks and additive phases',()=>{
  const s=summary(event()),a=arrival(s.p50);
  assert.equal(a.available,true);
  assert.deepEqual(a.parts.map(p=>p.ns),[709000,18000,3000,800,200,50,50,50,50]);
  assert.equal(a.raw_span_ns,709000);assert.equal(a.span_ns,709000);
  assert.equal(a.span_uncertainty_ns,8000);assert.equal(a.completion_stamp_to_book_ns,22200);
  assert.equal(a.model_identity_available,false);
  for(const p of ['p50','p99']) {
    const rows=ingressArrivalContributions(s);
    assert.equal(rows.reduce((n,r)=>n+r[p+'_ns'],0),s[p].total_ns);
    assert.ok(Math.abs(rows.reduce((n,r)=>n+r[p+'_percent'],0)-100)<1e-10);
  }
});
test('missing, inexact, inconsistent and cross-clock detail refuses without removing primary events',()=>{
  const changes=[
    r=>delete r.hardware,
    r=>r.hardware.source='kernel-software/realtime',
    r=>r.hardware.uncertainty_ns=null,
    r=>r.hardware.raw_ns=1791703394721419414,
    r=>r.hardware.raw_ns='1791703394720710413',
    r=>r.hardware.raw_ns='1791703394729419414',
    r=>r.hardware.raw_ns='999999999999999999999999999999',
    r=>r.hardware.raw_ns='-1',
    r=>r.hardware.raw_ns='1e19',
    r=>r.hardware.batch_to_message_ns=2999,
    r=>r.hardware.batch_to_message_ns=730001,
    r=>r.hardware.batch_to_message_ns=21000.5,
    r=>r.hardware.qualification={generation:'not-an-id'},
    r=>{r.hardware.qualification={generation:'8'};r.first_observed_read.hardware.qualification={generation:'7'};},
    r=>r.observed_last_read_to_message_ns=null,
    r=>r.socket.last=r.socket.first
  ];
  for(const change of changes) {
    const e=event();change(e.receive);const s=summary(e),a=arrival(s.p50);
    assert.equal(s.samples,1);assert.equal(s.p50.total_ns,731200);
    assert.equal(a.available,false,change.toString());assert.ok(a.reason);
    assert.deepEqual(ingressArrivalContributions(s),[]);
  }
  assert.equal(ingressArrival(null).available,false);
  assert.equal(ingressArrival(summary(event()).p50,'hardware').available,false);
  assert.equal(ingressArrival(summary(event()).p50).available,false);
});
test('raw/aligned difference retains independent allowances and supplied clock identities',()=>{
  const e=event(),r=e.receive;
  r.hardware.raw_ns=String(BigInt(r.hardware.raw_ns)+8000n);
  for(const s of [r.hardware,r.first_observed_read.hardware])s.qualification={generation:'9007199254740993'};
  const a=arrival(summary(e).p50);
  assert.equal(a.available,true);assert.equal(a.raw_span_ns,717000);assert.equal(a.span_ns,709000);
  assert.equal(a.model_identity_available,true);
  r.hardware.raw_ns=String(BigInt(r.hardware.raw_ns)+1n);
  assert.equal(arrival(summary(e).p50).available,false);
});
test('single reads require equal raw and aligned stamps and produce a zero arrival span',()=>{
  const e=event(),r=e.receive;
  r.socket.last=r.socket.first;r.first_observed_read.ended_to_message_ns=3000;
  r.first_observed_read.hardware={...r.hardware};
  const s=summary(e);
  assert.equal(arrival(s.p50).span_ns,0);assert.equal(arrival(s.p50).raw_span_ns,0);
  assert.equal(ingressArrivalContributions(s)[0].p50_percent,0);
  r.hardware.raw_ns=String(BigInt(r.hardware.raw_ns)+1n);
  assert.equal(arrival(summary(e).p50).available,false);
});
test('both table columns use the same granularity and never reselect an event for available detail',()=>{
  const a=event(1),b=event(2);b.trace.queue_ns=900;delete a.trace.decode_ns;
  const s=summary(b,a),before=JSON.stringify(s),rows=ingressArrivalContributions(s);
  assert.equal(s.p50.trace.id,'7:1');assert.equal(s.p99.trace.id,'7:2');
  assert.deepEqual(rows.map(r=>r.p50_ns),[709000,18000,3000,800,400]);
  assert.equal(rows.at(-1).label,'Decode + book processing');
  assert.equal(JSON.stringify(s),before);
  delete b.receive.hardware.raw_ns;const partial=summary(b,a);
  assert.equal(partial.p99.trace.id,'7:2');assert.equal(partial.p99.total_ns,731300);
  assert.deepEqual(ingressArrivalContributions(partial),[]);
  assert.equal(arrival(partial.p50).available,true);
});
