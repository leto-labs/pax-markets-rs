// Synthetic rejection/recovery states against the local fixture server only.
async page => {
  await page.unrouteAll({behavior:'ignoreErrors'});
  const assert=(condition,message)=>{if(!condition)throw new Error(message);};
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  let phase='none';
  await page.route('**/api/diagnostics',async route=>{
    const response=await route.fetch(),data=await response.json();
    data.transport=null;data.reports=null;
    await route.fulfill({response,json:data});
  });
  await page.route('**/api/state',async route=>{
    const response=await route.fetch(),data=await response.json(),s=data.state;
    s.view.mode='live';s.transport_connected=true;s.view.connected=true;
    s.view.last_rejection=phase==='none'?null:{
      id:'16:354013',observed_ns:Math.max(0,s.published_ns-60e9),market:'1',sequence:'1814864',
      reason:'post-event top of book mismatch <img src=x onerror="window.paxXss=true">',
    };
    for(const book of s.view.markets){
      book.status=phase==='pending'?'invalid':'live';
      book.generation=phase==='pending'?'16':'17';
      book.trace_id=phase==='pending'?'16:354012':'17:354014';
      book.sequence=phase==='pending'?'1814863':'1846626';
    }
    await route.fulfill({response,json:data});
  });
  for(const width of [360,1440]){
    await page.setViewportSize({width,height:900});
    for(const path of ['/','/latency']){
      phase='none';await page.goto('http://127.0.0.1:8080'+path);
      await page.waitForFunction(()=>document.querySelector('#source').textContent==='live');
      assert(await page.locator('#recovery-history').isHidden(),'empty history shown');
      phase='pending';
      await page.waitForFunction(()=>document.querySelector('#recovery-history').dataset.state==='pending');
      assert(await page.locator('#recovery-history').evaluate(el=>el.open&&el.classList.contains('rejection')),'pending recovery must be open and prominent');
      const reason=await page.locator('#last-rejection').textContent();
      assert(reason.includes('generation')||reason.includes('Generation'),'missing generation');
      assert(reason.includes('16, message 354013')&&!reason.includes('undefined'),'serialized ID was misread');
      assert(await page.locator('#last-rejection img').count()===0,'reason became HTML');
      phase='recovered';
      await page.waitForFunction(()=>document.querySelector('#recovery-history').dataset.state==='recovered');
      assert(await page.locator('#recovery-history').evaluate(el=>!el.open&&!el.classList.contains('rejection')),'recovered incident still shown as active failure');
      assert((await page.locator('#recovery-summary').textContent()).includes('resynchronized'),'missing recovery label');
      await page.locator('#recovery-summary').focus();await page.keyboard.press('Enter');
      await page.waitForTimeout(600);
      assert(await page.locator('#recovery-history').evaluate(el=>el.open&&document.activeElement===el.querySelector('summary')),'updates reset disclosure or focus');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'recovery details overflow viewport');
      await page.screenshot({path:`/tmp/pax-recovery-${path==='/'?'book':'latency'}-${width}.png`,fullPage:true});
    }
  }
  assert(errors.length===0,errors.join('\n'));
  await page.unrouteAll({behavior:'ignoreErrors'});
  return {pendingVisible:true,recoveredCollapsed:true,exactId:true,escapedReason:true,disclosurePreserved:true,mobile:true,errors};
}
