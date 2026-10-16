import test from 'node:test';
import assert from 'node:assert/strict';
import {EngineError, initiateWithRecovery, retryAfter, SampleEngine} from '../dist/api.mjs';
const options = () => ({deadline:Date.now()+10000,wait:async()=>{},random:()=>.5});

test('lost response retrieves original payment without another POST',async()=>{
  let posts=0,checks=0;
  const adapter={pay:async()=>{posts++;throw new EngineError('LOST',0);},inquire:async()=>{checks++;return {state:'PENDING',payment_id:'original'};}};
  assert.equal((await initiateWithRecovery(adapter,'order','same-key',{outcome:'SUCCEEDED'},options())).payment_id,'original');
  assert.equal(posts,1);assert.equal(checks,1);
});
test('NOT_STARTED replay uses identical order key and payload with Retry-After',async()=>{
  const calls=[],delays=[];let count=0;
  const body={outcome:'SUCCEEDED',delay_seconds:1,duplicates:3};
  const adapter={pay:async(...args)=>{calls.push(args);if(++count===1)throw new EngineError('BUSY',503,2000);return {payment_id:'one'};},inquire:async()=>({state:'NOT_STARTED',payment_id:null})};
  await initiateWithRecovery(adapter,'order','stable',body,{...options(),wait:async ms=>delays.push(ms)});
  assert.deepEqual(calls,[['order','stable',body],['order','stable',body]]);assert.equal(delays[0],2000);
});
test('inquiry failure never blindly replays payment',async()=>{
  let posts=0;
  const adapter={pay:async()=>{posts++;throw new EngineError('BUSY',503);},inquire:async()=>{throw new EngineError('UNAVAILABLE',503);}};
  await assert.rejects(initiateWithRecovery(adapter,'o','k',{},options()),/PAYMENT_INQUIRY_UNAVAILABLE/);assert.equal(posts,1);
});
test('authorization and deterministic conflicts are not automatically retried',async()=>{
  for(const status of [401,403,409,422]){
    let checks=0;const adapter={pay:async()=>{throw new EngineError('REJECTED',status);},inquire:async()=>{checks++;}};
    await assert.rejects(initiateWithRecovery(adapter,'o','k',{},options()),/REJECTED/);assert.equal(checks,0);
  }
});
test('automatic retries remain bounded to three total POSTs',async()=>{
  let posts=0,checks=0;
  const adapter={pay:async()=>{posts++;throw new EngineError('BUSY',503);},inquire:async()=>{checks++;return {state:'NOT_STARTED'};}};
  await assert.rejects(initiateWithRecovery(adapter,'o','k',{},options()),/BUSY/);assert.equal(posts,3);assert.equal(checks,3);
});
test('original expiry and Retry-After cannot be extended to allow a replay',async()=>{
  let posts=0;const adapter={pay:async()=>{posts++;throw new EngineError('BUSY',429,20000);},inquire:async()=>({state:'NOT_STARTED'})};
  await assert.rejects(initiateWithRecovery(adapter,'o','k',{},options()),/BUDGET_EXHAUSTED/);assert.equal(posts,1);
  await assert.rejects(initiateWithRecovery(adapter,'o','k',{}, {deadline:Date.now()-1}),/DEADLINE_REACHED/);assert.equal(posts,1);
  await assert.rejects(initiateWithRecovery(adapter,'o','k',{}, {}),/ORIGINAL_DEADLINE_REQUIRED/);
});
test('Retry-After supports seconds and HTTP dates',()=>{
  assert.equal(retryAfter('2'),2000);assert.equal(retryAfter('bad'),0);
  assert.equal(retryAfter('Wed, 21 Oct 2026 07:28:00 GMT',Date.parse('Wed, 21 Oct 2026 07:27:58 GMT')),2000);
});
function sample(scenario='normal') {const map=new Map();return new SampleEngine({scenario:()=>scenario,storage:{getItem:k=>map.get(k),setItem:(k,v)=>map.set(k,v)}});}
test('sample reserve replay returns same owner and original expiry',async()=>{
  const engine=sample();const first=await engine.reserve('e',['A1'],'key');const replay=await engine.reserve('e',['A1'],'key');
  assert.equal(first.order_id,replay.order_id);assert.equal(first.expires_at,replay.expires_at);
  await assert.rejects(engine.reserve('e',['A2'],'key'),/IDEMPOTENCY_CONFLICT/);
  await assert.rejects(engine.reserve('e',['A1'],'other'),/SEAT_UNAVAILABLE/);
});
test('sample lost-response scenario retains one original payment',async()=>{
  const engine=sample('recover');const hold=await engine.reserve('e',['A1'],'h');
  const result=await initiateWithRecovery(engine,hold.order_id,'p',{outcome:'SUCCEEDED'},options());
  assert.equal(result.payment_id,engine.state.orders[hold.order_id].payment.id);
  assert.equal(Object.keys(engine.state.orders).length,1);
});
