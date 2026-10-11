import test from 'node:test';
import assert from 'node:assert/strict';
import {duration,clockQualification,exclusive,alignClock,alignedDelay,browserStages,latestBookTrace,networkEstimate,firstPublication,FirstObservation,deliveryStages,reportStages,bookInputAge} from '../../src/presentation/assets/timing.mjs';

test('stage aggregation uses exclusive sums, never percentile sums',()=>{
  const parts=exclusive([{decode_ns:10,preflight_ns:2,mutation_ns:3,validation_ns:4,other_ns:1},{decode_ns:20,preflight_ns:4,mutation_ns:6,validation_ns:8,other_ns:2}]);
  assert.equal(parts.reduce((s,p)=>s+p.ns,0),60);
  assert.equal(parts[0].ns,30);
});
test('alignment reports uncertainty, session changes, expiration and impossible ordering',()=>{
  const clock=alignClock({session:'one',received_ms:112,sent_ms:114},10,20);
  assert.equal(clock.offset,98); assert.equal(clock.uncertainty,5);
  assert.deepEqual(alignedDelay(clock,'one',110e6,20,20),{estimate:8,lower:3,upper:13,uncertainty:5});
  assert.equal(alignedDelay(clock,'two',110e6,20,21),null);
  assert.equal(alignedDelay(clock,'one',110e6,20,40000),null);
  assert.equal(alignedDelay(clock,'one',110e6,20,19),null);
  assert.equal(alignedDelay(clock,'one',110e6,20,10020).uncertainty,6);
  assert.equal(alignedDelay(clock,'one',200e6,20,21),null);
  assert.equal(alignClock({session:'one',received_ms:10,sent_ms:30},1,2),null);
  assert.equal(alignClock({session:'one',received_ms:NaN,sent_ms:30},1,2),null);
});
test('sub-millisecond values stay readable and invalid times stay unavailable',()=>{
  assert.equal(duration(24),'24 ns'); assert.equal(duration(2300),'2.30 µs');
  assert.equal(duration(2e6),'2.00 ms'); assert.equal(duration(NaN),'unavailable');
});
test('clock model separates drift validation, original mapping and missing legacy fields',()=>{
  const model={method:'driver_extended',epoch:'9007199254740993',generation:2,age_ns:20e6,bracket_uncertainty_ns:1500,assumed_drift_ppm:100,mapping_deferred:true,rate_validated:false};
  const text=clockQualification(model);
  for(const word of ['epoch 9007199254740993','calibration age 20.00 ms','±1.50 µs','100 ppm','rate not validated','original read retained','PHC UTC accuracy','TCP packet association'])assert(text.includes(word));
  assert.equal(clockQualification(null),null);
  const {rate_validated,...older}=model;
  assert(clockQualification(older).includes('rate unreported (legacy)'));
  assert(clockQualification({...model,rate_validated:true,mapping_deferred:false}).includes('rate validated · mapping at receive'));
  const bad=clockQualification({...model,method:null,epoch:9007199254740992,age_ns:-1,bracket_uncertainty_ns:NaN,assumed_drift_ppm:undefined});
  assert(bad.includes('Clock model unavailable · epoch unavailable'));
  assert(!bad.includes('NaN')&&!bad.includes('undefined')&&!bad.includes('-1 ns'));
  assert(clockQualification({...model,method:'constructor',epoch:'18446744073709551616'}).includes('Clock model unavailable · epoch unavailable'));
});

test('browser stages cover every interval from receipt through the frame callback',()=>{
  const sample={setup_ns:100,parse_ns:200,dispatch_ns:300,dom_ns:400,frame_ns:500};
  assert.equal(browserStages(sample).reduce((s,p)=>s+p.ns,0),1500);
});

function correlatedView() {
  const markets=[
    {id:'1',status:'live',generation:'7',sequence:'100',trace_id:'7:9007199254740992'},
    {id:'2',status:'live',generation:'7',sequence:'200',trace_id:'7:9007199254740993'}
  ];
  return {markets,latency:{traces:markets.map(m=>({id:m.trace_id,market:m.id,generation:m.generation,sequence:m.sequence}))}};
}
test('latest contributing commit is selected exactly across markets in either order',()=>{
  const view=correlatedView();
  assert.equal(latestBookTrace(view).market.id,'2');
  view.markets.reverse();view.latency.traces.reverse();
  assert.equal(latestBookTrace(view).market.id,'2');
  view.markets[0].status='awaiting snapshot';
  assert.equal(latestBookTrace(view).market.id,'1');
  view.markets[1].status='invalid';
  assert.equal(latestBookTrace(view),null);
});
test('an unsampled or expired latest commit never falls back to an older trace',()=>{
  const view=correlatedView();
  view.latency.traces.pop();
  assert.equal(latestBookTrace(view),null);
  view.markets[1].trace_id=null;
  assert.equal(latestBookTrace(view),null);
});
test('trace identity requires matching generation, market and committed sequence',()=>{
  for (const key of ['id','market','generation','sequence']) {
    const view=correlatedView();view.latency.traces[1][key]='wrong';
    assert.equal(latestBookTrace(view),null,key);
  }
  for (const identity of ['wrong','6:9007199254740993','7:-1','7:1.5','7:1:2']) {
    const view=correlatedView();view.markets[1].trace_id=identity;
    assert.equal(latestBookTrace(view),null,identity);
  }
});

test('network model requires a fresh successful destination reply',()=>{
  const route={status:'complete',destination:'192.0.2.9',observed_unix_seconds:100,hops:[{address:'192.0.2.1',rtt_ms:1},{address:'192.0.2.9',rtt_ms:40}]};
  assert.deepEqual(networkEstimate(route,110),{rtt_ms:40,one_way_ms:20,age_seconds:10});
  assert.equal(networkEstimate(route,221),null);assert.equal(networkEstimate(route,99),null);
  for (const status of ['incomplete or filtered','timed out','unavailable']) assert.equal(networkEstimate({...route,status},110),null);
  assert.equal(networkEstimate({...route,hops:route.hops.slice(0,1)},110),null);
  for (const rtt_ms of [null,NaN,Infinity,-1]) assert.equal(networkEstimate({...route,hops:[{address:route.destination,rtt_ms}]},110),null);
  assert.equal(networkEstimate(null,110),null);
});

test('first publication uses exact commit and chronological checkpoints',()=>{
  const market={id:'2',trace_id:'1:9007199254740993',sequence:'18446744073709551615'};
  const trace={id:market.trace_id,received_ns:100,queue_ns:10,processing_ns:20};
  const packet={first_publications:{'2':{market:'2',trace_id:trace.id,sequence:market.sequence,published_ns:200,encoding_ns:30,ready_ns:250}}};
  const first=firstPublication(packet,market,trace);
  assert.deepEqual(first.parts.map(p=>p.ns),[10,20,70,20,30]);
  assert.equal(first.parts.reduce((sum,p)=>sum+p.ns,0),first.ready_ns-trace.received_ns);
  assert.equal(firstPublication(packet,{...market,sequence:'1'},trace),null);
  assert.equal(firstPublication(packet,market,{...trace,id:'1:1'}),null);
  for(const patch of [{published_ns:120},{ready_ns:201},{encoding_ns:NaN},{encoding_ns:-1}]) {
    assert.equal(firstPublication({first_publications:{'2':{...packet.first_publications['2'],...patch}}},market,trace),null);
  }
});
test('first browser observation freezes once and resets for exact identity or session',()=>{
  const saved=new FirstObservation();
  const market={id:'2',trace_id:'1:9007199254740993',sequence:'18446744073709551615'};
  assert.equal(saved.select('session',market),null);
  const original={parts:[{ns:123}],meta:'first'};
  saved.retain(original);
  assert.equal(saved.select('session',market),original);
  assert.equal(saved.retain({meta:'later'}),original);
  for(const changed of [{...market,id:'1'},{...market,sequence:'1'},{...market,trace_id:'2:1'}]) {
    saved.select('session',market);saved.retain(original);
    assert.equal(saved.select('session',changed),null);
  }
  saved.retain(original);assert.equal(saved.select('restart',market),null);
  saved.retain(original);saved.clear();assert.equal(saved.select('restart',market),null);
});

test('delivery stages partition dispatch and reject absent or inconsistent markers',()=>{
  const trace={queue_ns:60,delivery:{transport_ns:10,handoff_ns:20,preparation_ns:30}};
  assert.deepEqual(deliveryStages([trace,trace]).map(p=>p.ns),[20,40,60]);
  for(const value of [[],[{}],[trace,{}],[{...trace,queue_ns:61}],[{...trace,delivery:{...trace.delivery,handoff_ns:-1}}]])assert.deepEqual(deliveryStages(value),[]);
});
test('report stages exclude encoding and age includes snapshot-to-publication delay',()=>{
  assert.deepEqual(reportStages({snapshot_ns:10,handoff_ns:20,preparation_ns:30}).map(p=>p.ns),[10,20,30]);
  assert.deepEqual(reportStages(null),[]);
  const report={published_ns:200,view:{mode:'live',observed_ns:100}};
  assert.equal(bookInputAge(report,{update_age_ns:50}),150);
  assert.equal(bookInputAge({...report,view:{mode:'replay',observed_ns:100}},{update_age_ns:50}),50);
  assert.equal(bookInputAge(report,{update_age_ns:null}),null);
  assert.equal(bookInputAge({...report,published_ns:0},{update_age_ns:50}),null);
});

test('receive work counts shared operations once and never fills missing intervals', async()=>{
  const {receiveStages,receiveBoundary}=await import('../../src/presentation/assets/timing.mjs');
  const first={id:'1:1',shared:true,work:{socket_ns:10,tls_ns:20,websocket_ns:30,pending_ns:1000}};
  const buffered={id:'1:2',shared:true,buffered:true,work:{socket_ns:0,tls_ns:0,websocket_ns:5,pending_ns:0}};
  assert.equal(receiveStages([first,first,buffered]).reduce((sum,s)=>sum+s.ns,0),65);
  assert.deepEqual(receiveStages([]),[]);
  assert.deepEqual(receiveStages([{id:'1:3'}]),[]);
  const trace={id:'1:1',queue_ns:2,processing_ns:3};
  assert.equal(receiveBoundary(first,trace,'hardware'),null);
  first.software={source:'kernel-software/realtime',status:'batch',batch_to_message_ns:40,uncertainty_ns:7};
  assert.equal(receiveBoundary(first,trace,'software').ns,45);
  assert.equal(receiveBoundary(first,{...trace,id:'2:1'},'software'),null);
  first.software.batch_to_message_ns=null;
  assert.equal(receiveBoundary(first,trace,'software'),null);
});

test('partial processing and receive observations never synthesize missing phases',async()=>{
  const {receiveStages}=await import('../../src/presentation/assets/timing.mjs');
  assert.deepEqual(exclusive([{decode_ns:null,preflight_ns:null,mutation_ns:null,validation_ns:null,other_ns:null}]),[]);
  const parts=exclusive([{decode_ns:10,preflight_ns:null,mutation_ns:null,validation_ns:null,other_ns:20}]);
  assert.deepEqual(parts.map(p=>p.ns),[10,20]);
  assert.deepEqual(receiveStages([{id:'1:1',complete_policy_window:false,work:{socket_ns:1,tls_ns:2,websocket_ns:3}}]),[]);
  const trace={id:'1:1',received_ns:1,queue_ns:null,processing_ns:null};
  const market={id:'1',trace_id:'1:1',sequence:'1'};
  const packet={first_publications:{'1':{market:'1',trace_id:'1:1',sequence:'1',published_ns:10,ready_ns:12,encoding_ns:1}}};
  assert.equal(firstPublication(packet,market,trace),null);
});

test('browser sample windows preserve unchanged phases and do not depend on display cadence',async()=>{
  const {SampleWindow,rateLabel}=await import('../../src/presentation/assets/timing.mjs');
  const sample=new SampleWindow({every:5,width:2,phase:0});
  assert.deepEqual(Array.from({length:10},(_,i)=>sample.observe()?i+1:null).filter(Boolean),[1,5,6,10]);
  assert.equal(sample.configure({every:5,width:2,phase:0}),false);
  assert.equal(sample.eligible,10);assert.equal(sample.selected,4);
  assert.equal(sample.configure({every:0,width:0,phase:0}),true);
  assert.equal(sample.observe(),false);assert.equal(sample.eligible,1);
  assert.equal(rateLabel(sample.rate),'off');
  assert.throws(()=>sample.configure({every:2,width:3,phase:0}));
  assert.equal(sample.rate.every,0);
});

test('read internals count each delivery once and withhold unmeasured phases',async()=>{
  const {readStages}=await import('../../src/presentation/assets/timing.mjs');
  const full={eligible:1,samples:1,exclusive_ns:10};
  const partial={eligible:2,samples:1,exclusive_ns:null};
  const trace={id:'1:1',work:{read_detail:{readiness:full,preparation:partial,syscall:full,metadata:full,delivery:full}}};
  assert.deepEqual(readStages([trace,trace]).map(p=>p.ns),[10,10,10,10]);
  assert.deepEqual(readStages([{...trace,complete_policy_window:false}]),[]);
  assert.deepEqual(readStages([{id:'1:2',work:{}}]),[]);
});
