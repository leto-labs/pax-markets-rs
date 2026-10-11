import test from 'node:test';
import assert from 'node:assert/strict';
import {LatencyHistory} from '../../src/presentation/assets/history.mjs';
const core=(source='first-hardware')=>({source,boundary_label:'first read window',stale:false,samples:100,
  receive_revisions:[1],processing_revisions:[2],p50:{total_ns:1000,trace:{generation:'1'}},p99:{total_ns:2000,trace:{generation:'1'}}});
test('bounds observation rate and retention without combining window percentiles',()=>{
  const h=new LatencyHistory();
  for(let at=0;at<600000;at+=250)h.observe(at,'one',core());
  assert.ok(h.points.length<=301);assert.equal(h.points.at(-1).p50,1000);
  const recent=h.view(599750,60);assert.equal(recent.points,60);assert.equal(recent.max,2000);
  assert.throws(()=>h.view(600000,10),/range/);
});
test('stale, missing and hidden-tab intervals break lines rather than inventing zeros',()=>{
  const h=new LatencyHistory();h.observe(0,'one',core());h.observe(1000,'one',core());
  const stale=core();stale.stale=true;h.observe(2000,'one',stale);h.observe(3000,'one',core());
  h.observe(10000,'one',core());assert.equal(h.view(10000,60).segments.length,3);
  const absent=core();absent.p50=null;h.observe(11000,'one',absent);
  assert.equal(h.view(11000,60).latest.p50,null);
});
test('session, source, policy and generation transitions start independent series',()=>{
  for(const change of [(s,c)=>['two',c],(s,c)=>[s,{...c,source:'first-software'}],
    (s,c)=>[s,{...c,receive_revisions:[3]}],(s,c)=>[s,{...c,processing_revisions:[4]}],
    (s,c)=>[s,{...c,p50:{...c.p50,trace:{generation:'2'}}}]]){
    const h=new LatencyHistory();h.observe(0,'one',core());h.observe(1000,'one',core());
    const [session,candidate]=change('one',core());h.observe(2000,session,candidate);
    assert.equal(h.points.length,1);assert.equal(h.points[0].at,2000);
  }
});
test('invalid durations remain unavailable while measured zero stays valid',()=>{
  const h=new LatencyHistory();const zero=core();zero.p50.total_ns=0;zero.p99.total_ns=0;
  h.observe(0,'one',zero);assert.equal(h.view(0,60).latest.p50,0);
  const bad=core();bad.p99.total_ns=Number.MAX_SAFE_INTEGER+1;
  h.observe(1000,'one',bad);assert.equal(h.view(1000,60).latest.p99,null);
  assert.equal(h.observe(NaN,'one',core()),false);
});
test('failed fetch gaps replace a recent point and never extend a live measurement',()=>{
  const h=new LatencyHistory();h.observe(0,'one',core());h.gap(100);
  assert.equal(h.points.length,1);assert.equal(h.view(100,60).latest.p50,null);
  h.observe(1100,'one',core());assert.equal(h.view(1100,60).segments.length,1);
});
test('alignment budgets preserve totals and scale, clipping display below zero',()=>{
  const h=new LatencyHistory(),value=core();
  value.p50.uncertainty_ns=2000;value.p99.uncertainty_ns=500;
  h.observe(0,'one',value);h.observe(1000,'one',value);
  const view=h.view(1000,60);
  assert.equal(view.latest.p50,1000);assert.equal(view.latest.p99,2000);
  assert.equal(view.max,3000);
  assert.deepEqual(view.bands.p50[0].map(p=>[p.lower,p.upper]),[[0,3000],[0,3000]]);
  assert.deepEqual(view.bands.p99[0].map(p=>[p.lower,p.upper]),[[1500,2500],[1500,2500]]);
});
test('absent, invalid and overflowing budgets stay unshaded without hiding latency',()=>{
  for(const budget of [undefined,null,-1,NaN,Infinity,'5',Number.MAX_SAFE_INTEGER]){
    const h=new LatencyHistory(),value=core();value.p50.uncertainty_ns=budget;
    h.observe(0,'one',value);const view=h.view(0,60);
    assert.equal(view.latest.p50,1000);assert.equal(view.latest.p50Uncertainty,null);
    assert.deepEqual(view.bands.p50,[]);assert.equal(view.max,2000);
  }
});
test('budget omissions and stale gaps do not join shaded regions',()=>{
  const h=new LatencyHistory(),value=core();value.p50.uncertainty_ns=0;
  h.observe(0,'one',value);h.observe(1000,'one',core());h.observe(2000,'one',value);
  h.gap(3000);h.observe(4000,'one',value);
  const view=h.view(4000,60);
  assert.equal(view.segments.length,2);assert.equal(view.bands.p50.length,3);
  assert.deepEqual(view.bands.p50.map(b=>b.map(p=>p.at)),[[0],[2000],[4000]]);
  assert.equal(view.latest.p50Uncertainty,0);
});
