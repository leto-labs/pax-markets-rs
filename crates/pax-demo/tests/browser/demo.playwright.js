// Run against `serve --fixture` with playwright-cli run-code --filename=...
async page => {
  const waitFor=async (...args)=>{try{return await page.waitForFunction(...args);}catch(e){throw new Error(String(args[0])+' '+e.message);}};
  await page.unrouteAll({behavior:'ignoreErrors'});
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  let stateRequests=0;page.on('request',request=>{if(request.url().endsWith('/api/state'))stateRequests++;});
  const assert=(condition,message)=>{if(!condition)throw new Error(message);};
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    for(const path of ['/','/latency']){
      await page.goto('http://127.0.0.1:8080'+path);
      await waitFor(()=>document.getElementById('source').textContent.includes('fixture replay'));
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),`page overflow: ${width} ${path}`);
      if(path==='/'){
        await waitFor(()=>document.querySelectorAll('tbody tr').length>=20);
        assert(await page.locator('h2').first().textContent()==='BTC-USDC','metadata name');
      }else{
        assert(!await page.locator('#sdk-details').evaluate(el=>el.open),'processing detail should start collapsed');
        assert(!await page.locator('#presentation-details').evaluate(el=>el.open),'presentation detail should start collapsed');
        await page.locator('#pause').click();
        const beforeDisclosure=stateRequests;
        await page.locator('#sdk-details > summary').focus();
        await page.keyboard.press('Enter');
        await waitFor(()=>document.getElementById('sdk-details').open);
        await waitFor(()=>document.querySelectorAll('#rust-chart button').length===5);
        assert(await page.locator('#sdk-details > summary').evaluate(el=>el.getBoundingClientRect().height>=44),'detail disclosure is not touch sized');
        await page.locator('#presentation-details > summary').click();
        await waitFor(()=>Number(document.querySelector('#rust-chart svg').getAttribute('width'))===Math.max(280,document.querySelector('#rust-chart').clientWidth));
        assert(stateRequests===beforeDisclosure,'opening diagnostics initiated a state request');
        await page.locator('#rust-chart button').first().click();
        assert((await page.locator('#rust-detail').textContent()).includes('Decode:'),'touch-accessible stage details');
        await page.locator('#trace-select').selectOption('aggregate');
        assert((await page.locator('#trace-meta').textContent()).includes('summed exclusive'),'aggregate label');
        const aggregateDuration=(await page.locator('#rust-chart button').first().textContent()).split(' · ')[1];
        assert((await page.locator('#rust-detail').textContent()).includes(`Decode: ${aggregateDuration}`),'selected detail retained an old trace duration');
        assert((await page.locator('#rust-detail').textContent()).includes('event samples'),'selected detail retained an old trace title');
        await page.locator('#zoom').selectOption('8');
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'zoom leaked outside chart');
        assert(await page.locator('#rust-chart').evaluate(el=>el.scrollWidth>el.clientWidth),'zoom did not create bounded chart scroll');
        await page.locator('#rust-chart').evaluate(el=>{el.scrollLeft=140;});
        await page.evaluate(()=>{
          window.paxFocusedStage=document.querySelector('#rust-chart button');
          window.paxFocusedStage.focus({preventScroll:true});
          window.dispatchEvent(new Event('resize'));
        });
        assert(await page.locator('#rust-chart').evaluate(el=>el.scrollLeft===140),'chart redraw lost horizontal position');
        assert(await page.evaluate(()=>document.activeElement===window.paxFocusedStage),'chart resize interrupted keyboard focus');
        await page.locator('#zoom').selectOption('1');
        await page.locator('#pause').click();
        await page.evaluate(()=>{
          window.paxFocusedStage=document.querySelector('#rust-chart button');
          window.paxFocusedStage.focus({preventScroll:true});
          window.paxFocusTrace=document.querySelector('#trace-select').options[2].value;
        });
        await waitFor(()=>document.querySelector('#trace-select').options[2].value!==window.paxFocusTrace);
        assert(await page.evaluate(()=>document.activeElement===window.paxFocusedStage),'live chart publication interrupted keyboard focus');
        assert(await page.evaluate(()=>{
          window.paxFocusedStage.click();
          return document.querySelector('#rust-detail').textContent.includes('Decode: '+window.paxFocusedStage.textContent.split(' · ')[1]);
        }),'reused stage control retained an old duration');
        await page.locator('#trace-select').selectOption('latest');
        await page.locator('#trace-select').focus();
        await page.evaluate(()=>{
          window.paxFocusedOption=document.querySelector('#trace-select').options[2];
          window.paxSelectorTrace=document.querySelector('#trace-meta').textContent;
        });
        await waitFor(()=>document.querySelector('#trace-meta').textContent!==window.paxSelectorTrace);
        assert(await page.evaluate(()=>window.paxFocusedOption.isConnected && document.querySelector('#trace-select').options[2]===window.paxFocusedOption),'live publications replaced focused trace choices');
        await page.locator('#zoom').focus();
        assert(await page.evaluate(()=>!window.paxFocusedOption.isConnected),'trace choices did not refresh after leaving the selector');
      }
      await page.screenshot({path:`/tmp/pax-${path==='/'?'book':'latency'}-${width}.png`,fullPage:true});
    }
  }
  let extremePayload;
  await page.route('**/api/state',async route=>{
    const response=await route.fetch();const payload=await response.json();
    payload.state.view.markets[0].sequence='18446744073709551615';
    for(const side of ['asks','bids'])for(const level of payload.state.view.markets[0][side])level.price='123456789012345678901234567890.123456789012';
    payload.state.view.markets[0].name='<img src=x onerror="window.paxXss=true">';
    if(!extremePayload&&payload.state.view.markets[0].bids.length&&payload.state.view.markets[0].asks.length)extremePayload=payload;
    if(extremePayload)extremePayload.state.update_id=payload.state.update_id;
    await route.fulfill({response,json:extremePayload||payload});
  });
  await page.setViewportSize({width:360,height:900});
  await page.goto('http://127.0.0.1:8080/');
  await waitFor(()=>document.querySelector('#books').textContent.includes('18446744073709551615'));
  await waitFor(()=>document.querySelector('td')?.textContent==='123456789012345678901234567890.123456789012');
  assert(await page.locator('#books img').count()===0,'metadata HTML was not escaped');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'large price escaped the viewport');
  assert(await page.locator('.ladders>div').first().evaluate(el=>el.scrollWidth>el.clientWidth),'large exact price has no scroll access');
  assert(await page.locator('td').first().evaluate(el=>el.scrollWidth<=el.clientWidth),'large exact price was truncated inside its cell');
  await page.locator('.ladders>div').first().evaluate(el=>{window.paxScrolledLadder=el;window.paxBookAge=document.querySelector('#display-freshness').textContent;el.scrollLeft=40;});
  await waitFor(()=>document.querySelector('#display-freshness').textContent!==window.paxBookAge);
  assert(await page.evaluate(()=>document.querySelector('.ladders>div')===window.paxScrolledLadder),'book publication replaced the ladder');
  assert(await page.locator('.ladders>div').first().evaluate(el=>el.scrollLeft===40),'book publication lost horizontal position');
  await page.unroute('**/api/state');
  await page.route('**/api/state',route=>route.abort());
  await waitFor(()=>document.getElementById('source').textContent==='STALE DISPLAY');
  await page.unroute('**/api/state');
  // Use an explicitly synthetic live-shaped publication to exercise cross-clock
  // correlation without exchange access. The fixture itself stays replay-labeled.
  let restarted=false,unsampled=false,quiet=false,quietCommit;
  await page.route('**/api/state',async route=>{
    const response=await route.fetch();const payload=await response.json();
    const report=payload.state;report.view.mode='live';
    if(restarted)report.session='synthetic-restarted-session';
    const first=report.view.markets[0];first.status='live';first.trace_id=`${first.generation}:9007199254740992`;
    const newer={...first,id:'2',trace_id:`${first.generation}:9007199254740993`};
    report.view.markets=[first,newer];
    report.view.latency.traces=(unsampled?[first]:[first,newer]).map(market=>({id:market.trace_id,population:'event',market:market.id,generation:market.generation,sequence:market.sequence,
      received_ns:report.published_ns-1000000,queue_ns:1000,processing_ns:2000,
      decode_ns:1000,preflight_ns:300,mutation_ns:200,validation_ns:300,other_ns:200,delivery:{transport_ns:100,handoff_ns:500,preparation_ns:400,pending:3},bookkeeping_ns:125,pending_gap_ns:300}));
    report.report_work={captured_ns:report.published_ns-80000,snapshot_ns:30000,handoff_ns:10000,preparation_ns:40000,skipped:2};
    report.view.observed_ns=report.published_ns-80000;
    payload.first_publications=Object.fromEntries(report.view.markets.map(m=>[m.id,{market:m.id,trace_id:m.trace_id,sequence:m.sequence,published_ns:report.published_ns,encoding_ns:payload.encoding_ns,ready_ns:payload.ready_ns}]));
    if(quiet){
      quietCommit??={markets:report.view.markets,traces:report.view.latency.traces,first:payload.first_publications};
      report.view.markets=quietCommit.markets;report.view.latency.traces=quietCommit.traces;payload.first_publications=quietCommit.first;
    }
    await route.fulfill({response,json:payload});
  });
  await page.goto('http://127.0.0.1:8080/latency');
  await page.locator('#sdk-details > summary').click();
  await page.locator('#presentation-details > summary').click();
  await waitFor(()=>document.querySelector('#overall-chart svg')?.getAttribute('aria-label').includes('Local receipt → frame'));
  assert((await page.locator('#overall-meta').textContent()).includes('clock uncertainty'),'estimated delivery lost uncertainty');
  assert(await page.locator('#delivery-chart button').count()===3,'exclusive delivery stages missing');
  assert(await page.locator('#report-chart button').count()===3,'report preparation stages missing');
  assert((await page.locator('#delivery-meta').textContent()).includes('Gaps overlap'),'overlapping gaps need a non-additive label');
  assert((await page.locator('#report-meta').textContent()).includes('2 periodic reports skipped'),'report backpressure missing');
  await page.locator('#delivery-chart button').nth(1).click();
  assert((await page.locator('#delivery-detail').textContent()).includes('Delivery handoff: 500 ns'),'delivery control stale or inaccessible');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'delivery panels overflow mobile viewport');

  assert((await page.locator('#overall-meta').textContent()).includes('market 2'),'latest committed market was not selected');
  const beforeQuiet=await page.locator('#overall-meta').textContent();
  quiet=true;
  await waitFor(meta=>document.getElementById('overall-meta').textContent!==meta,beforeQuiet);
  await waitFor(()=>document.querySelector('#overall-chart svg')?.getAttribute('aria-label').includes('Local receipt → frame'));
  const frozenMeta=await page.locator('#overall-meta').textContent();
  const frozenChart=await page.locator('#overall-chart svg').getAttribute('aria-label');
  const age=await page.locator('#overall-age').textContent();
  await waitFor(age=>document.getElementById('overall-age').textContent!==age,age);
  assert(await page.locator('#overall-meta').textContent()===frozenMeta,'quiet refresh replaced first observation');
  assert(await page.locator('#overall-chart svg').getAttribute('aria-label')===frozenChart,'quiet refresh inflated timeline');
  quiet=false;
  await waitFor(meta=>document.getElementById('overall-meta').textContent!==meta,frozenMeta);
  unsampled=true;
  await waitFor(()=>document.getElementById('overall-meta').textContent.includes('no retained trace'));
  assert(await page.locator('#overall-chart svg').count()===0,'unsampled latest commit fell back to an older trace');
  assert((await page.locator('#overall-age').textContent()).includes('Book input age'),'unsampled commit lost independent freshness');
  unsampled=false;
  await waitFor(()=>document.getElementById('overall-meta').textContent.includes('market 2'));
  await page.evaluate(()=>{
    window.paxHidden=true;
    Object.defineProperty(document,'hidden',{configurable:true,get:()=>window.paxHidden});
    document.dispatchEvent(new Event('visibilitychange'));
  });
  assert((await page.locator('#browser-meta').textContent()).includes('Tab hidden'),'hidden tab not labeled');
  assert((await page.locator('#browser-detail').textContent())==='Select a stage for its duration and method.','unavailable chart retained old details');
  assert((await page.locator('#alignment').textContent()).includes('No valid'),'hidden tab retained alignment');
  await page.evaluate(()=>{window.paxHidden=false;document.dispatchEvent(new Event('visibilitychange'));});
  await waitFor(()=>document.getElementById('browser-meta').textContent.includes('tab visible'));
  restarted=true;
  await waitFor(()=>document.getElementById('alignment').textContent.includes('No valid'));
  assert(!(await page.locator('#overall-chart svg').getAttribute('aria-label')).includes('Local receipt → frame'),'restarted server retained cross-clock span');
  await page.unroute('**/api/state');
  await page.reload();
  await waitFor(()=>document.getElementById('source').textContent.includes('fixture replay'));
  await page.locator('#presentation-details > summary').click();
  await waitFor(()=>document.getElementById('overall-meta').textContent.includes('Cross-clock end-to-end spans are unavailable'));
  assert(!(await page.locator('#overall-age').textContent()).includes('at publication'),'replay book age crossed clock domains');
  assert(errors.length===0,errors.join('\n'));
  return {mobile:360,desktop:1440,pages:2,chartDetails:true,zoomContained:true,scrollPreserved:true,focusPreserved:true,selectorChoicesPreserved:true,exactIntegers:true,escapedMetadata:true,staleDisplay:true,alignedEstimate:true,frozenFirstObservation:true,bookAgeSeparate:true,latestMarketCorrelation:true,unsampledCorrelation:true,hiddenTabInvalidation:true,sessionInvalidation:true,errors};
}
