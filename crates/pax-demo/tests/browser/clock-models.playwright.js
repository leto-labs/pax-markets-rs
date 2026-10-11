// Qualified model records are synthetic; fixture mode never claims real ingress.
async page => {
  await page.unrouteAll({behavior:'ignoreErrors'});
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  const assert=(condition,message)=>{if(!condition)throw new Error(message);};
  let legacy=false;
  await page.route('**/api/state',async route=>{
    const response=await route.fetch(),payload=await response.json(),view=payload.state.view;
    view.mode='live';view.markets[0].status='live';view.latency.stale=false;
    const generation=view.markets[0].generation;
    const model=epoch=>({generation:'17',epoch,method:'user_bracket',age_ns:1000000,
      bracket_uncertainty_ns:100,assumed_drift_ppm:100,rate_validated:false,mapping_deferred:false});
    view.latency.traces=[0,1,2].map(index=>({id:`${generation}:${index+1}`,generation,
      market:view.markets[0].id,sequence:String(index+1),population:'event',received_ns:10000+index,
      policy_revision:1,queue_ns:20,processing_ns:150,decode_ns:100,preflight_ns:20,mutation_ns:10,validation_ns:10,other_ns:10}));
    view.latency.receive=view.latency.traces.map((trace,index)=>({id:trace.id,received_ns:trace.received_ns,revision:1,complete_policy_window:true,
      socket:{first:'10',last:'11'},observed_last_read_to_message_ns:100,
      hardware:{source:'nic-hardware/phc',status:'synthetic completion',batch_to_message_ns:1000,uncertainty_ns:200,qualification:model(99)},
      first_observed_read:{batch:'10',ended_to_message_ns:300,
        hardware:{source:'nic-hardware/phc',status:'synthetic first window',batch_to_message_ns:7000+index*1000,uncertainty_ns:200,...(legacy?{}:{qualification:model(7+index)})}}}));
    await route.fulfill({response,json:payload});
  });
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    for(const path of ['/','/latency']){
      legacy=false;await page.goto('http://127.0.0.1:8080'+path);
      await page.waitForFunction(()=>document.querySelector('#ingress-clock-p50')?.textContent.includes('epoch 8 / connection 17'));
      assert((await page.locator('#cards .card b').first().textContent())==='8.17 µs','primary metric changed');
      assert((await page.locator('#ingress-clock-p99').textContent()).includes('epoch 9 / connection 17'),'p99 used another event model');
      const summary=page.locator('#ingress-clock-models > summary');
      assert((await summary.boundingBox()).height>=44,'clock detail control is not touch sized');
      await summary.click();
      assert((await page.locator('#ingress-clock-p50').textContent()).includes('rate not validated'),'assumed rate presented as verified');
      assert(!((await page.locator('#ingress-clock-p50').textContent()).includes('epoch 99')),'completion model replaced first-read model');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'expanded clock details overflow');
      await page.screenshot({path:`/tmp/pax-clock-models-${path==='/'?'book':'latency'}-${width}.png`,fullPage:true});
      if(path==='/latency'){
        await page.locator('#ingress-source').selectOption('hardware');
        await page.waitForFunction(()=>document.querySelector('#ingress-clock-p50')?.textContent.includes('epoch 99'));
        await page.locator('#ingress-source').selectOption('first-hardware');
      }
      legacy=true;
      await page.waitForFunction(()=>document.querySelector('#ingress-clock-p50')?.textContent.includes('details unavailable'));
      assert(!((await page.locator('#ingress-clock-p50').textContent()).includes('epoch 99')),'legacy model details invented');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'legacy clock details overflow');
    }
  }
  assert(errors.length===0,'clock detail page errors: '+errors.join('; '));
  return {errors,viewports:[360,1440],qualified_data:'synthetic only',checks:'matched first/completion models, percentile events, legacy absence, touch size and expanded viewport bounds'};
}
