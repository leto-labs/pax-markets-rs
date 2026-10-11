// Run against serve --fixture on :8080. Explicitly synthetic diagnostic metadata.
async page => {
  await page.unrouteAll({behavior:'ignoreErrors'});
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  const assert=(condition,message)=>{if(!condition)throw new Error(message);};
  let enabled=true,phaseMode=false,qualification=null;
  await page.route('**/api/state',async route=>{
    const response=await route.fetch();const payload=await response.json();
    const traces=payload.state.view.latency.traces;
    payload.state.view.latency.receive=enabled?traces.map((t,index)=>({id:t.id,received_ns:t.received_ns,
      plaintext:{first:'9007199254740992',last:'9007199254740993'},socket:{first:'4',last:'5'},
      work:{socket_ns:phaseMode?100:index?0:100,tls_ns:index?0:200,websocket_ns:30,pending_ns:index?0:1000000,polls:1,reads:phaseMode?1:index?0:1,
        ...(phaseMode?{read_detail:{...Object.fromEntries(['readiness','preparation','syscall','metadata','delivery'].map(k=>[k,{eligible:1,samples:1,exclusive_ns:10}])),would_block:0,initialized_bytes:0,copied_bytes:8}}:{})},
      shared:!phaseMode,buffered:phaseMode?false:index>0,coverage:'synthetic shared read window; exact record association unavailable',lost:2,sample_every:8,
      software:{source:'kernel-software/realtime',status:'clock alignment or observation stale',batch_to_message_ns:null,uncertainty_ns:null},
      hardware:{source:'unavailable',status:'PHC read permission denied',batch_to_message_ns:null,uncertainty_ns:null,...(qualification?{qualification}:{})}})):[];
    await route.fulfill({response,json:payload});
  });
  for(const width of [360,1440]){
    qualification={method:'driver_extended',epoch:'9007199254740993',generation:2,age_ns:20e6,bracket_uncertainty_ns:1500,assumed_drift_ppm:100,mapping_deferred:true,rate_validated:false};
    await page.setViewportSize({width,height:900});
    await page.goto('http://127.0.0.1:8080/latency');
    await page.locator('#sdk-details > summary').click();
    await page.waitForFunction(()=>document.querySelector('#receive-meta').textContent.includes('selected traces have receive metadata'));
    await page.locator('#pause').click();
    await page.locator('#trace-select').selectOption('aggregate');
    await page.locator('#sdk-details details > summary').click();
    assert((await page.locator('#receive-coverage').textContent()).includes('9007199254740992'),'batch identity lost integer precision');
    assert((await page.locator('#receive-boundaries').textContent()).includes('clock alignment or observation stale'),'stale clock not exposed');
    assert((await page.locator('#receive-boundaries').textContent()).includes('PHC read permission denied'),'hardware unavailable not exposed');
    assert((await page.locator('#receive-boundaries').textContent()).includes('rate not validated'),'drift validation absent');
    assert((await page.locator('#receive-boundaries').textContent()).includes('original read retained'),'deferred mapping boundary absent');
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'receive panel overflows viewport');
    await page.locator('#receive-chart button').last().click();
    assert((await page.locator('#receive-detail').textContent()).includes('WebSocket / observer:'),'receive details inaccessible');
    await page.locator('#zoom').selectOption('8');
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'receive zoom overflows viewport');
    await page.screenshot({path:`/tmp/pax-receive-${width}.png`,fullPage:true});
    await page.locator('#pause').click();
  }
  delete qualification.rate_validated;
  await page.waitForFunction(()=>document.querySelector('#receive-boundaries').textContent.includes('rate unreported (legacy)'));
  qualification.method=null;
  await page.waitForFunction(()=>document.querySelector('#receive-boundaries').textContent.includes('Clock model unavailable'));
  phaseMode=true;
  await page.waitForFunction(()=>document.querySelectorAll('#readwork-chart button').length===5);
  await page.locator('#readwork-chart button').nth(2).click();
  assert((await page.locator('#readwork-detail').textContent()).includes('recvmsg:'),'read internals have no accessible detail');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'read internals overflow viewport');
  enabled=false;
  await page.waitForFunction(()=>document.querySelector('#receive-meta').textContent.includes('disabled, unsampled or unavailable'));
  assert(await page.locator('#receive-chart svg').count()===0,'disabled receive stage fabricated a bar');
  assert((await page.locator('#receive-detail').textContent()).includes('Select a stage'),'disabled stage retained stale details');
  assert(errors.length===0,errors.join('\n'));
  await page.unrouteAll({behavior:'ignoreErrors'});
  return {widths:[360,1440],sharedWork:true,readInternals:true,exactIds:true,staleClock:true,unavailableHardware:true,disabled:true,errors};
}
