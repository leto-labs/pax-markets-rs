// Run against serve --fixture on :8080. Qualified SDK windows below are synthetic.
async page => {
  await page.unrouteAll({behavior:'ignoreErrors'});
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  const assert=(condition,message)=>{if(!condition)throw new Error(message);};
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    for(const path of ['/','/latency']){
      await page.goto('http://127.0.0.1:8080'+path);
      await page.waitForFunction(()=>document.querySelector('#cards .card b')?.textContent==='Unavailable');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'empty history viewport overflow');
      if(path==='/latency'){
        assert((await page.locator('#history-meta').textContent()).includes('Current window unavailable'),'fixture invented ingress history');
        assert(await page.locator('#history-chart svg path[d]:not([d=""])').count()===0,'empty history invented a line');
      }
    }
  }
  let stale=false;
  await page.route('**/api/state',async route=>{
    const response=await route.fetch(),payload=await response.json(),view=payload.state.view;
    view.mode='live';view.markets[0].status='live';view.latency.stale=stale;
    const generation=view.markets[0].generation;
    view.latency.traces=[1000,2000,3000].map((before,index)=>({id:`${generation}:${index+1}`,generation,
      market:view.markets[0].id,sequence:String(index+1),population:'event',received_ns:10000+index,
      policy_revision:1,queue_ns:20,processing_ns:150,decode_ns:100,preflight_ns:20,mutation_ns:10,validation_ns:10,other_ns:10}));
    view.latency.receive=view.latency.traces.map((trace,index)=>({id:trace.id,received_ns:trace.received_ns,revision:1,complete_policy_window:true,
      observed_last_read_to_message_ns:100,software:{source:'kernel-software/realtime',status:'synthetic read-batch proxy',batch_to_message_ns:(index+1)*1000,uncertainty_ns:5},
      hardware:{source:'unavailable',batch_to_message_ns:null,uncertainty_ns:null}}));
    await route.fulfill({response,json:payload});
  });
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    await page.goto('http://127.0.0.1:8080/latency');
    await page.waitForFunction(()=>document.querySelector('#history-meta')?.textContent.includes('Latest window: p50 2.17 µs'));
    await page.waitForFunction(()=>document.querySelector('#history-chart path[data-line="p50"]')?.getAttribute('d')?.includes('L'));
    assert(await page.locator('#history-chart path[data-band][d]:not([d=""])').count()===2,'qualified clock budgets are unshaded');
    assert((await page.locator('#history-meta').textContent()).includes('±5 ns'),'latest window omitted clock budget');
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'qualified history viewport overflow');
    assert(!await page.locator('#history-chart svg').getAttribute('viewBox').then(value=>/NaN|Infinity/.test(value)),'invalid plot dimensions');
    await page.getByRole('combobox',{name:'Range',exact:true}).selectOption('60');
    assert((await page.locator('#history-meta').textContent()).includes('in 1 min'),'range control did not update');
    await page.getByRole('combobox',{name:'Ingress source',exact:true}).selectOption('hardware');
    assert((await page.locator('#history-meta').textContent()).includes('Current window unavailable'),'source transition retained incompatible values');
    assert(await page.locator('#history-chart path[d]:not([d=""])').count()===0,'source transition joined old history');
    await page.getByRole('combobox',{name:'Ingress source',exact:true}).selectOption('software');
    assert((await page.locator('#history-meta').textContent()).includes('1 tab observations'),'source transition did not reset history');
    await page.screenshot({path:`/tmp/pax-history-${width}.png`,fullPage:true});
  }
  stale=true;
  await page.waitForFunction(()=>document.querySelector('#history-meta')?.textContent.includes('Current window unavailable'));
  assert(errors.length===0,'history browser errors: '+errors.join('; '));
  return {errors,viewports:[360,1440],qualified_data:'synthetic only',checks:'empty/qualified trends, range, source reset, stale gaps and viewport bounds'};
}
