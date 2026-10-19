/* Optional UI QA. Needs Playwright package and a local Chrome executable. */
const {chromium}=require('playwright');
const fs=require('fs');
const path=require('path');
const assert=require('assert/strict');
(async()=>{
  const executable=process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  const browser=await chromium.launch({executablePath:executable,headless:true});
  const context=await browser.newContext({viewport:{width:1440,height:1050},reducedMotion:'reduce'});
  const page=await context.newPage();
  const errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  page.on('response',r=>{if(r.status()>=400)errors.push(r.status()+' '+r.url());});
  const base=process.env.COURSE_URL || 'http://127.0.0.1:8765/';
  const url=rel=>base+rel.split('/').map(encodeURIComponent).join('/');
  const out=path.join(__dirname,'결과');
  fs.mkdirSync(out,{recursive:true});
  await page.goto(url('이론/index.html'));
  await page.evaluate(()=>document.fonts.ready);
  assert.equal(await page.locator('.lesson-row').count(),12);
  await page.screenshot({path:path.join(out,'home-desktop.png'),fullPage:true});
  await page.locator('[data-search]').fill('Outbox');
  assert.equal(await page.locator('.lesson-row:visible').count(),1);
  await page.locator('[data-search]').fill('there-is-no-such-lesson');
  assert.equal(await page.locator('.lesson-row:visible').count(),0);
  assert.match(await page.locator('.search-feedback').textContent(),/검색 결과가 없습니다/);
  await page.locator('[data-search]').fill('');
  console.log('PASS catalogue and search');
  for(let i=1;i<=12;i++){
    const id=String(i).padStart(2,'0'); const level=i<5?'초급':i<9?'중급':'고급';
    await page.goto(url('이론/'+level+'/'+id+'.html'));
    assert.equal(await page.locator('.lesson-section').count(),6);
    const image=page.locator('.diagram img');
    await image.evaluate(el=>el.decode());
    const before=await page.locator('[data-step-message]').textContent();
    await page.locator('[data-step=next]').click();
    assert.notEqual(await page.locator('[data-step-message]').textContent(),before);
    await page.locator('[data-step=reset]').click();
    assert.equal(await page.locator('[data-step-message]').textContent(),before);
    const correct=await page.locator('#lesson-data').evaluate(el=>JSON.parse(el.textContent).correct);
    await page.locator('[data-answer="'+correct+'"]').click();
    assert.match(await page.locator('.quiz-feedback').textContent(),/^정답입니다/);
    await page.locator('[data-answer="'+((correct+1)%3)+'"]').click();
    assert.match(await page.locator('.quiz-feedback').textContent(),/^다시 생각/);
    if(i===3){await page.locator('[data-tool] input').fill('4');assert.match(await page.locator('.tool-output').textContent(),/배정 없음/);}
    if(i===5){await page.locator('[data-action=process]').click();await page.locator('[data-action=restart]').click();await page.locator('[data-action=process]').click();assert.match(await page.locator('.tool-output').textContent(),/중복/);}
    if(i===8){await page.locator('[data-tool] input').fill('1');assert.match(await page.locator('.tool-output').textContent(),/진행 불가/);}
    if(i===9){await page.locator('[data-action=begin]').click();await page.locator('[data-action=abort]').click();assert.match(await page.locator('.tool-output').textContent(),/0건/);await page.locator('[data-action=begin]').click();await page.locator('[data-action=commit]').click();assert.match(await page.locator('.tool-output').textContent(),/1건/);}
    if(i===10){for(const a of ['save','send','crash','send'])await page.locator('[data-action='+a+']').click();assert.match(await page.locator('.tool-output').textContent(),/기록 수: 2/);}
    if(i===1){await page.locator('[data-complete]').click();await page.reload();assert.equal(await page.locator('[data-complete]').getAttribute('aria-pressed'),'true');}
    if(i===6)await page.screenshot({path:path.join(out,'lesson-desktop.png'),fullPage:true});
    await page.setViewportSize({width:390,height:844});
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'mobile overflow lesson '+id);
    if(i===1){await page.locator('.menu-toggle').click();assert.equal(await page.locator('.menu-toggle').getAttribute('aria-expanded'),'true');await page.locator('[data-menu-close]').click();assert.equal(await page.locator('.menu-toggle').getAttribute('aria-expanded'),'false');await page.locator('.menu-toggle').click();await page.keyboard.press('Escape');await page.evaluate(()=>scrollTo(0,0));await page.screenshot({path:path.join(out,'lesson-mobile.png'),fullPage:true});}
    await page.setViewportSize({width:1440,height:1050});
  }
  await page.goto(url('이론/index.html'));
  assert.equal(await page.locator('.lesson-row.done').count(),1);
  assert.match(await page.locator('[data-resume]').getAttribute('href'),/02.html$/);
  await page.setViewportSize({width:390,height:844});
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'mobile overflow home');
  await page.screenshot({path:path.join(out,'home-mobile.png'),fullPage:true});
  // Offline file:// entry works without fetch / modules / server.
  const {pathToFileURL}=require('url');
  await page.goto(pathToFileURL(path.join(__dirname,'..','이론','초급','01.html')).href);
  await page.locator('.diagram img').evaluate(el=>el.decode());
  await page.locator('[data-step=next]').click();
  assert.match(await page.locator('[data-step-count]').textContent(),/^2 /);
  assert.deepEqual(errors,[]);
  console.log('PASS 12 lessons: images, flow player, quiz, 5 simulators, progress persistence');
  console.log('PASS 390px responsive layout, mobile navigation and offline file://');
  console.log('PASS no browser errors or failed HTTP responses');
  await browser.close();
})().catch(e=>{console.error(e);process.exit(1);});
