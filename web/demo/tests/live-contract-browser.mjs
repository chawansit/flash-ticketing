import {createRequire} from 'node:module';
import fs from 'node:fs';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.ENCORE_PLAYWRIGHT_MODULE || 'playwright');
const browser=await chromium.launch({headless:true,...(process.env.ENCORE_BROWSER_EXECUTABLE?{executablePath:process.env.ENCORE_BROWSER_EXECUTABLE}:{})});
const page=await browser.newPage({viewport:{width:1366,height:1000}});
const base=process.env.ENCORE_BASE_URL||'http://127.0.0.1:4080';
if(!['127.0.0.1','localhost'].includes(new URL(base).hostname))throw Error('Local preview only');
const id='10000000-0000-4000-8000-000000000001',order='20000000-0000-4000-8000-000000000001',hold='30000000-0000-4000-8000-000000000001',command='40000000-0000-4000-8000-000000000001';
let payments=0,holdPosts=0,queries=0,failEvents=false,loseHoldResponse=false,failCommand=false;const keys=[],holdKeys=[];
const errors=[];page.on('pageerror',e=>errors.push(e.message));
await page.route('**/demo-config',route=>route.fulfill({json:{engineConfigured:true}}));
await page.route('**/engine/**',async route=>{
 const request=route.request(),path=new URL(request.url()).pathname.replace('/engine','');
 let value,status=200;
 if(path==='/v1/events'){value=failEvents?{code:'ENGINE_UNAVAILABLE'}:[{id,name:'Contract test concert',currency:'THB'}];if(failEvents)status=503;}
 else if(path.endsWith('/layout'))value={seats:[{seat_id:'A1',price:120000},{seat_id:'A2',price:120000}]};
 else if(path.endsWith('/availability'))value={seats:[{seat_id:'A1',status:'AVAILABLE'},{seat_id:'A2',status:'AVAILABLE'}]};
 else if(path==='/v1/holds'){holdPosts++;holdKeys.push(request.headers()['idempotency-key']);value={hold_id:hold,command_id:command,order_id:order,expires_at:new Date(Date.now()+120000).toISOString(),persistence_status:'PENDING'};status=202;if(loseHoldResponse){loseHoldResponse=false;status=502;value={code:'ENGINE_RESPONSE_UNKNOWN'};}}
 else if(path.includes('/reservation-commands/'))value=failCommand?{persistence_status:'FAILED',failure_code:'RESERVATION_FAILED'}:{persistence_status:'DURABLE'};
 else if(path.endsWith('/payments')){payments++;keys.push(request.headers()['idempotency-key']);value={code:'ENGINE_RESPONSE_UNKNOWN'};status=502;}
 else if(path.endsWith('/payment-operation')){queries++;if(request.headers()['idempotency-key']!==keys[0])throw Error('Payment inquiry changed identity');value={state:'PENDING',payment_id:'original-payment'};}
 else if(path==='/v1/orders/'+order)value={status:'FULFILLED',tickets:[{id:'original-ticket',seat_id:'A1'}]};
 else if(request.method()==='DELETE'&&path.includes('/v1/holds/'))value={status:'released'};
 else if(path==='/metrics')value={httpRequests:10,httpErrors:1,dbUnavailable:0};
 else throw Error('Unexpected contract path '+path);
 await route.fulfill({status,json:value});
});
await page.goto(base);await page.getByRole('button',{name:/Seat A1,/}).waitFor();
async function connect(){await page.locator('#connect-button').click();await page.locator('#engine-token').fill('synthetic-customer-token');await page.locator('#live-button').click();}
await connect();await page.getByRole('button',{name:/Seat A1,/}).waitFor();
await page.getByRole('button',{name:/Seat A1,/}).click();await page.getByRole('button',{name:'Reserve these seats'}).click();
await page.getByRole('button',{name:'Pay with simulator'}).waitFor();await page.getByRole('button',{name:'Pay with simulator'}).click();
await page.getByText('You’re going. Make it a night.').waitFor();
if(payments!==1||queries!==1||holdPosts!==1)throw Error('Recovery generated duplicate initiation');
const leaked=await page.evaluate(()=>Object.values(sessionStorage).some(s=>s.includes('synthetic-customer-token')));
if(leaked)throw Error('Customer token persisted');
await page.reload();await page.getByRole('button',{name:/Seat A1,/}).waitFor();await connect();
await page.getByText('You’re going. Make it a night.').waitFor();if(payments!==1||holdPosts!==1)throw Error('Refresh created a new operation');
await page.getByRole('button',{name:'Book more seats'}).click();await page.getByRole('button',{name:/Seat A1,/}).waitFor();
failEvents=true;await connect();await page.getByText('Waiting for an event',{exact:true}).waitFor();
if(await page.locator('[data-seat]').count())throw Error('Live failure left sample seats visible');
failEvents=false;await page.reload();await page.getByRole('button',{name:/Seat A1,/}).waitFor();await connect();
await page.getByRole('button',{name:/Seat A2,/}).click();loseHoldResponse=true;
await page.getByRole('button',{name:'Reserve these seats'}).click();
await page.getByRole('button',{name:'Check original reservation'}).waitFor();
if(!(await page.locator('#event-picker').isDisabled()))throw Error('Ambiguous hold allowed event switch');
await page.reload();await page.getByRole('button',{name:/Seat A1,/}).waitFor();await connect();
await page.getByRole('button',{name:'Check original reservation'}).click();
await page.getByRole('button',{name:'Pay with simulator'}).waitFor();
if(holdKeys.at(-1)!==holdKeys.at(-2))throw Error('Ambiguous hold changed identity after refresh');
await page.getByRole('button',{name:'Release these seats'}).click();await page.getByRole('button',{name:/Seat A1,/}).waitFor();failCommand=true;
await page.getByRole('button',{name:/Seat A1,/}).click();await page.getByRole('button',{name:'Reserve these seats'}).click();
await page.getByText('Reservation could not be completed',{exact:true}).waitFor();if(await page.getByRole('button',{name:'Pay with simulator'}).count())throw Error('Failed reservation enabled payment');
if(errors.length)throw Error(JSON.stringify(errors));
fs.mkdirSync('tmp',{recursive:true});fs.writeFileSync('tmp/encore-live-contract-check.json',JSON.stringify({pass:true,scope:'Browser UI with intercepted API contract responses; not a real engine or financial test',lostResponseInquiry:true,samePaymentKey:true,noDuplicateInitiation:true,refreshRetrievesExistingOrder:true,tokenNotPersisted:true,failedLiveConnectionDoesNotShowSampleSeats:true,ambiguousHoldIdentitySurvivesRefresh:true,failedDurabilityBlocksPayment:true,pageErrors:errors},null,2));
console.log('Live-adapter browser contract checks passed.');
await browser.close();
