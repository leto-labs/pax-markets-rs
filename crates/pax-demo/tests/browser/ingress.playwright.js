// Run against serve --fixture on :8080. Qualified metadata below is synthetic.
async page => {
  await page.unrouteAll({behavior:'ignoreErrors'});
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  const assert=(condition,message)=>{if(!condition)throw new Error(message);};
  let unavailable=false,partial=false,early=false;
  await page.route('**/api/state',async route=>{
    const response=await route.fetch(),payload=await response.json(),v=payload.state.view;
    v.mode='live';v.markets[0].status='live';
    const generation=v.markets[0].generation;
    v.latency.traces=[{before:1000,decode:800},{before:2000,decode:10},{before:3000,decode:100}].map((s,i)=>({
      id:`${generation}:${i+1}`,generation,market:v.markets[0].id,sequence:String(i+1),population:'event',received_ns:10000+i,
      policy_revision:1,queue_ns:20,processing_ns:s.decode+50,decode_ns:partial?null:s.decode,preflight_ns:20,mutation_ns:10,validation_ns:10,other_ns:10}));
    v.latency.receive=v.latency.traces.map((t,i)=>({id:t.id,received_ns:t.received_ns,revision:0,complete_policy_window:true,
      socket:{first:'1',last:'1'},observed_last_read_to_message_ns:100,
      software:{source:'kernel-software/realtime',raw_ns:'1791703394720715414',status:unavailable?'clock stale':'read-batch proxy',batch_to_message_ns:unavailable?null:(i+1)*1000,uncertainty_ns:5},
      hardware:{source:'unavailable',status:'PHC unavailable',batch_to_message_ns:null,uncertainty_ns:null}}));
    if(early)for(const [i,r] of v.latency.receive.entries()){
      r.socket.last='2';
      r.first_observed_read={batch:'1',ended_to_message_ns:6000,
      software:{source:'kernel-software/realtime',raw_ns:'1791703394720710414',status:'first observed window',batch_to_message_ns:(i+1)*1000+5000,uncertainty_ns:10},
      hardware:{source:'nic-hardware/phc',status:'first observed window',batch_to_message_ns:(i+1)*1000+7000,uncertainty_ns:100}};
    }
    await route.fulfill({response,json:payload});
  });
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    for(const path of ['/','/latency']){
      await page.goto('http://127.0.0.1:8080'+path);
      await page.waitForFunction(()=>document.querySelector('#cards .card b')?.textContent==='2.08 µs');
      assert((await page.locator('#cards .card small').first().textContent())==='Kernel RX → book p50','SDK metric is not first');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'ingress page overflow');
      if(path==='/latency'){
        assert((await page.locator('section h2').first().textContent())==='SDK ingress → book commit','SDK panel is not first');
        assert((await page.locator('#ingress-table tr').last().textContent()).includes('3.17 µs100.00%'),'p99 total/percent mismatch');
        assert((await page.locator('#ingress-meta').textContent()).includes('3/3 retained accepted events'),'coverage missing');
        await page.locator('#ingress-event').selectOption('p99');
        await page.locator('#ingress-chart button').first().click();
        assert((await page.locator('#ingress-detail').textContent()).includes('2.90 µs'),'wrong percentile event in flame graph');
        await page.locator('#ingress-source').selectOption('hardware');
        assert((await page.locator('#cards .card b').first().textContent())==='Unavailable','hardware fell back to software');
        assert(await page.locator('#ingress-chart svg').count()===0,'hardware invented a chart');
        await page.locator('#ingress-source').selectOption('software');
        await page.locator('#sdk-details > summary').click();
        await page.locator('#zoom').selectOption('8');
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'ingress zoom escaped viewport');
        assert(await page.locator('#browser-chart').count()===1&&await page.locator('#report-chart').count()===1,'secondary diagnostics removed');
        await page.locator('#zoom').selectOption('1');
        await page.screenshot({path:`/tmp/pax-ingress-${width}.png`,fullPage:true});
      }
    }
  }
  early=true;
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    for(const path of ['/','/latency']){
      await page.goto('http://127.0.0.1:8080'+path);
      await page.waitForFunction(()=>document.querySelector('#cards .card b')?.textContent==='9.08 µs');
      assert((await page.locator('#cards .card small').first().textContent())==='NIC first observed RX → book p50','first observed source not primary');
      assert((await page.locator('#cards .card span').first().textContent()).includes('First observed read-window proxy'),'first boundary qualifier missing');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'first-window page overflow');
      if(path==='/latency'){
        assert((await page.locator('#ingress-table tr').last().textContent()).includes('10.17 µs100.00%'),'first-window phases do not sum to event total');
        const table=await page.locator('#ingress-table').textContent();
        assert(table.includes('First → last observed read end')&&table.includes('Last read end → complete message')&&table.includes('5.90 µs'),'associated read chronology missing');
        await page.locator('#ingress-chart button').nth(1).click();
        assert((await page.locator('#ingress-detail').textContent()).includes('Not isolated network wait'),'read chronology caveat missing');
        await page.screenshot({path:`/tmp/pax-read-chronology-${width}.png`,fullPage:true});
        await page.locator('#ingress-breakdown').selectOption('arrivals');
        assert((await page.locator('#ingress-arrival-meta').textContent()).includes('arrival detail unavailable'),'missing completion stamps fabricated arrival phases');
        assert((await page.locator('#cards .card b').first().textContent())==='9.08 µs','arrival detail changed headline population');
        assert(await page.locator('#ingress-chart svg').count()===0,'missing arrival stamps invented a chart');
        await page.locator('#ingress-source').selectOption('first-software');
        assert((await page.locator('#cards .card b').first().textContent())==='7.08 µs','first sources conflated');
        assert((await page.locator('#ingress-table').textContent()).includes('First → completion associated RX stamp5.00 µs'),'arrival phases absent');
        assert((await page.locator('#ingress-arrival-meta').textContent()).includes('raw same-clock cross-check 5.00 µs'),'raw cross-check missing');
        assert((await page.locator('#ingress-table tr').last().textContent()).includes('8.17 µs100.00%'),'arrival partition changed total');
        await page.locator('#ingress-chart button').first().click();
        assert((await page.locator('#ingress-detail').textContent()).includes('not isolated network delay'),'arrival caveat missing');
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'arrival view overflow');
        await page.screenshot({path:`/tmp/pax-arrival-${width}.png`,fullPage:true});
        await page.locator('#ingress-source').selectOption('hardware');
        assert((await page.locator('#cards .card b').first().textContent())==='Unavailable','completion choice silently changed boundary');
        await page.locator('#ingress-breakdown').selectOption('reads');
        await page.locator('#ingress-source').selectOption('software');
      }
      await page.screenshot({path:`/tmp/pax-first-window-${width}-${path==='/'?'book':'latency'}.png`,fullPage:true});
    }
  }
  early=false;
  await page.waitForFunction(()=>document.querySelector('#cards .card b')?.textContent==='2.08 µs');
  partial=true;
  await page.waitForFunction(()=>document.querySelector('#ingress-table').textContent.includes('Decode + book processing'));
  assert((await page.locator('#cards .card b').first().textContent())==='2.08 µs','partial stages changed the total population');
  unavailable=true;
  await page.waitForFunction(()=>document.querySelector('#cards .card b').textContent==='Unavailable');
  assert(await page.locator('#ingress-table tr').count()===0,'unavailable source retained a stale table');
  assert(errors.length===0,errors.join('\n'));
  await page.unrouteAll({behavior:'ignoreErrors'});
  return {widths:[360,1440],pages:2,headlineFirst:true,firstWindow:true,associatedArrivals:true,sourceSeparation:true,quantileEvents:true,partialStages:true,unavailable:true,secondaryPreserved:true,errors};
}
