// Synthetic browser-only cases against the local offline fixture server.
async page => {
  await page.unrouteAll({behavior:'ignoreErrors'});
  await page.goto('about:blank');
  const assert=(condition,message)=>{if(!condition)throw new Error(message);};
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  let publication=0,frozen=false,delayNext=false,release,waiting=false,diagnosticsFail=false;
  await page.route('**/api/clock',route=>route.abort());
  await page.route('**/api/diagnostics',async route=>{
    if(diagnosticsFail)return route.abort();
    const response=await route.fetch(),data=await response.json();
    data.transport=null;data.reports=null;data.display.sampling={every:0,phase:0,width:0};
    await route.fulfill({response,json:data});
  });
  await page.route('**/api/state',async route=>{
    const response=await route.fetch(),data=await response.json();
    if(!frozen)publication++;
    data.state.update_id=String(publication);data.state.view.mode='live';data.state.transport_connected=true;
    data.state.view.applied=String(publication);
    data.state.report_work={captured_ns:data.state.published_ns-80000,snapshot_ns:30000,handoff_ns:10000,preparation_ns:40000,skipped:publication};
    for(const market of data.state.view.markets)market.update_age_ns=35e9;
    if(delayNext){delayNext=false;waiting=true;await new Promise(resolve=>release=resolve);waiting=false;}
    try{await route.fulfill({response,json:data});}catch{/* The browser may have canceled this response. */}
  });
  await page.evaluate(()=>window.name='pax-initial-hidden-test');
  await page.addInitScript(()=>{
    if(window.name!=='pax-initial-hidden-test')return;
    window.paxHidden=true;
    Object.defineProperty(document,'hidden',{configurable:true,get:()=>window.paxHidden});
  });
  await page.goto('http://127.0.0.1:8080/');
  await page.waitForTimeout(500);
  assert(publication===0,'initial hidden load started state polling');
  await page.evaluate(()=>{window.name='';window.paxHidden=false;document.dispatchEvent(new Event('visibilitychange'));});
  await page.waitForFunction(()=>document.querySelector('[data-field=age]')?.textContent.includes('35.'));
  assert((await page.locator('#source').textContent())==='live','quiet market appeared disconnected');
  assert((await page.locator('#display-freshness').textContent()).includes('Display refreshed'),'missing independent freshness');
  await page.locator('.depth-details > summary').first().click();
  await page.locator('.depth-details > summary').first().focus();
  await page.evaluate(()=>window.paxDisclosure=document.activeElement);
  await page.waitForTimeout(600);
  assert(await page.evaluate(()=>window.paxDisclosure===document.activeElement&&window.paxDisclosure.parentElement.open),'publication replaced focused liquidity disclosure');
  await page.goto('http://127.0.0.1:8080/latency');
  await page.waitForFunction(()=>document.querySelector('#source').textContent==='live');
  await page.waitForFunction(()=>[...document.querySelectorAll('#measurement-policy tr')].some(row=>row.cells[0].textContent==='browser'&&row.cells[1].textContent==='off'));
  await page.locator('#presentation-details > summary').click();
  await page.waitForFunction(()=>document.querySelector('#report-chart button'));
  const initialReport=await page.locator('#report-meta').textContent();
  await page.waitForFunction(before=>document.querySelector('#report-meta').textContent!==before,initialReport);
  assert(!await page.locator('#sdk-details').evaluate(node=>node.open),'test opened processing details');
  delayNext=true;
  await page.waitForTimeout(400);
  assert(waiting,'delayed request did not start');
  await page.locator('#pause').click();
  const frozenHealth=await page.locator('#health').textContent();
  release();await page.waitForTimeout(500);
  assert((await page.locator('#source').textContent())==='DISPLAY PAUSED','late response overrode pause');
  assert(await page.locator('#health').textContent()===frozenHealth,'late response changed frozen report');
  const beforeResume=publication;
  await page.locator('#pause').click();
  await page.waitForFunction(()=>document.querySelector('#source').textContent==='live');
  assert(publication>beforeResume,'resume did not refresh');
  await page.evaluate(()=>{
    window.paxHidden=true;
    Object.defineProperty(document,'hidden',{configurable:true,get:()=>window.paxHidden});
    document.dispatchEvent(new Event('visibilitychange'));
  });
  const hiddenHealth=await page.locator('#health').textContent();
  await page.waitForTimeout(350);
  assert(await page.locator('#health').textContent()===hiddenHealth,'hidden display changed');
  await page.evaluate(()=>{window.paxHidden=false;document.dispatchEvent(new Event('visibilitychange'));});
  await page.waitForFunction(previous=>document.querySelector('#health').textContent!==previous,hiddenHealth);
  frozen=true;diagnosticsFail=true;
  await page.waitForFunction(()=>document.querySelector('#source').textContent==='STALE DISPLAY');
  assert((await page.locator('#health').textContent()).includes('publication stopped'),'frozen publisher was not detected without browser clocks or diagnostics');
  frozen=false;diagnosticsFail=false;
  await page.waitForFunction(()=>document.querySelector('#source').textContent==='live');
  assert(errors.length===0,errors.join('\n'));
  await page.unrouteAll({behavior:'ignoreErrors'});
  return {quietBookFreshDisplay:true,focusedDisclosurePreserved:true,lateReplyAfterPauseIgnored:true,resumeRefresh:true,hiddenDisplayFrozen:true,staleWithoutOptionalClocks:true,recovery:true,errors};
}
