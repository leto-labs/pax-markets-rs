// Run against a fixture demo. BASE defaults to the documented local demo port.
async page => {
  const base='http://127.0.0.1:8080';
  const assert=(ok,message)=>{if(!ok)throw new Error(message);};
  await page.goto('about:blank');
  await page.unrouteAll({behavior:'ignoreErrors'});
  let enabled=false,clocks=0;
  const errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  page.on('request',r=>{if(r.url().endsWith('/api/clock'))clocks++;});
  await page.route('**/api/diagnostics',async route=>{
    const response=await route.fetch();const status=await response.json();
    status.display.sampling=enabled?{every:2,width:1,phase:0}:{every:0,width:0,phase:0};
    await route.fulfill({response,json:status});
  });
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    await page.goto(base+'/latency');
    await page.waitForFunction(()=>document.querySelector('#browser-meta').textContent.includes('off by policy'));
    await page.waitForFunction(()=>document.querySelectorAll('#measurement-policy tr').length>=8);
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'measurement table leaked outside viewport');
    assert(await page.locator('#measurement-health').textContent().then(s=>s.includes('Build:')),'compiled capabilities missing');
    assert(await page.locator('#browser-chart button').count()===0,'disabled browser retained old sample');
  }
  assert(clocks===0,'browser-off still requests alignment');
  enabled=true;
  await page.waitForFunction(()=>document.querySelector('#measurement-policy').textContent.includes('1/2, phase 0'));
  await page.waitForFunction(()=>document.querySelector('#browser-meta').textContent.includes('tab visible'));
  assert(clocks>0,'reenabled browser has no alignment');
  assert(errors.length===0,errors.join('\n'));
  await page.unrouteAll({behavior:'ignoreErrors'});
  return {mobile:360,desktop:1440,independentStatus:true,browserOffSkipsAlignment:true,mixedBrowserSampling:true,errors};
}
