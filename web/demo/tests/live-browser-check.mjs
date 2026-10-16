// Contract-double browser checks. No cloud engine or payment gateway is contacted.
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.ENCORE_PLAYWRIGHT_MODULE||'playwright');
const base=process.env.ENCORE_BASE_URL||'http://127.0.0.1:4080';
if(!['localhost','127.0.0.1'].includes(new URL(base).hostname))throw Error('Local preview only');
const browser=await chromium.launch({headless:true,...(process.env.ENCORE_BROWSER_EXECUTABLE?{executablePath:process.env.ENCORE_BROWSER_EXECUTABLE}:{})});
const event='10000000-0000-4000-8000-000000000001';
const order='20000000-0000-4000-8000-000000000001';
const hold={hold_id:'30000000-0000-4000-8000-000000000001',order_id:order,command_id:'40000000-0000-4000-8000-000000000001',expires_at:new Date(Date.now()+120000).toISOString(),persistence_status:'PENDING'};
const checks=[];
try {
  for(const scenario of ['recover','lost-hold','failed-command','expired','unavailable']) {
    const page=await browser.newPage();const calls=[];let holdPosts=0,paymentPosts=0,fulfilled=false;
    const errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.route('**/demo-config',r=>r.fulfill({json:{engineConfigured:true}}));
    await page.route('**/engine/**',async route=>{
      const req=route.request(),path=new URL(req.url()).pathname;
      calls.push({path,method:req.method(),key:req.headers()['idempotency-key'],auth:req.headers().authorization,body:req.postData()});
      const respond=(json,status=200)=>route.fulfill({status,json});
      if(scenario==='unavailable')return respond({code:'UNAVAILABLE'},503);
      if(path==='/engine/v1/events')return respond([{id:event,name:'Contract test concert',currency:'THB'}]);
      if(path.endsWith('/layout'))return respond({seats:[{seat_id:'A1',price:10000},{seat_id:'A2',price:10000}]});
      if(path.endsWith('/availability'))return respond({seats:[{seat_id:'A1',status:'AVAILABLE'},{seat_id:'A2',status:'AVAILABLE'}]});
      if(path==='/engine/v1/holds'){
        holdPosts++;
        if(scenario==='lost-hold'&&holdPosts===1)return route.abort('failed');
        return respond({...hold,...(scenario==='expired'?{expires_at:new Date(Date.now()+1800).toISOString(),persistence_status:'DURABLE'}:{})},202);
      }
      if(path.includes('/reservation-commands/'))return respond({...hold,persistence_status:scenario==='failed-command'?'FAILED':'DURABLE',failure_code:'RESERVATION_FAILED'});
      if(path.endsWith('/payments')){paymentPosts++;return route.abort('failed');}
      if(path.endsWith('/payment-operation')){fulfilled=true;return respond({state:'PENDING',payment_id:'original-payment'});}
      if(path===`/engine/v1/orders/${order}`)return respond({status:fulfilled?'FULFILLED':'PENDING',tickets:fulfilled?[{id:'original-ticket',seat_id:'A1'}]:[]});
      if(path.endsWith('/metrics'))return respond({httpRequests:0,httpErrors:0,dbUnavailable:0});
      throw Error('Unexpected contract route '+path);
    });
    const connect=async()=>{
      await page.locator('#connect-button').click();await page.locator('#engine-token').fill('synthetic-customer-token');
      await page.locator('#engine-event').fill(event);await page.locator('#live-button').click();
    };
    await page.goto(base);await page.getByRole('button',{name:/Seat A1,/}).waitFor();await connect();
    if(scenario==='unavailable'){
      await page.getByRole('heading',{name:'Waiting for an event'}).waitFor();
      assert.equal(await page.locator('[data-seat]').count(),0);
      assert.match(await page.locator('#mode-badge').innerText(),/Live engine/);
    } else {
      await page.getByRole('button',{name:/Seat A1,/}).click();await page.locator('#reserve-button').click();
      if(scenario==='lost-hold'){
        await page.getByRole('button',{name:/Check original reservation/}).waitFor();
        assert.ok(await page.locator('#event-picker').isDisabled());
        const first=calls.find(c=>c.path.endsWith('/holds'));
        await page.reload();await page.getByRole('button',{name:/Seat A1,/}).waitFor();await connect();
        await page.getByRole('button',{name:/Check original reservation/}).click();
        await page.locator('#pay-button').waitFor();
        const posts=calls.filter(c=>c.path.endsWith('/holds'));
        assert.equal(posts.length,2);assert.equal(posts[1].key,first.key);assert.equal(posts[1].body,first.body);
      } else if(scenario==='failed-command'){
        await page.getByText('Reservation could not be completed',{exact:true}).waitFor();
        assert.equal(await page.locator('#pay-button').count(),0);assert.equal(paymentPosts,0);
        await page.locator('#new-booking').click();await page.locator('#reserve-button').waitFor();
      } else if(scenario==='expired'){
        await page.getByText('The reservation expired',{exact:true}).waitFor({timeout:6000});
        assert.equal(await page.locator('#pay-button').count(),0);assert.equal(paymentPosts,0);
      } else {
        await page.locator('#pay-button').waitFor();await page.locator('#pay-button').click();
        await page.getByText('You’re going. Make it a night.').waitFor();
        assert.equal(paymentPosts,1);
        const post=calls.find(c=>c.path.endsWith('/payments')),inquiry=calls.find(c=>c.path.endsWith('/payment-operation'));
        assert.equal(inquiry.key,post.key);assert.equal(post.auth,'Bearer synthetic-customer-token');
        assert.ok(calls.indexOf(inquiry)>calls.indexOf(post));
        assert.ok(await page.locator('#event-picker').isDisabled());
        const stored=await page.evaluate(()=>JSON.stringify({...localStorage,...sessionStorage}));
        assert.ok(!stored.includes('synthetic-customer-token'));
        await page.reload();await page.getByRole('button',{name:/Seat A1,/}).waitFor();
        assert.match(await page.locator('#mode-badge').innerText(),/Sample mode/);
        await connect();await page.getByText('You’re going. Make it a night.').waitFor();assert.equal(paymentPosts,1);
      }
    }
    assert.deepEqual(errors,[]);checks.push(scenario);await page.close();
  }
  fs.mkdirSync('tmp',{recursive:true});
  fs.writeFileSync('tmp/encore-live-browser-check.json',JSON.stringify({pass:true,scope:'browser with HTTP contract doubles; no real engine',checks},null,2));
  console.log('Live adapter contract checks passed: '+checks.join(', '));
} finally {await browser.close();}
