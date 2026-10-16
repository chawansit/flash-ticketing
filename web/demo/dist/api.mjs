export class EngineError extends Error {
  constructor(code, status = 0, retryAfterMs = 0) { super(code); this.code = code; this.status = status; this.retryAfterMs = retryAfterMs; }
}
export const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
export const operationKey = () => crypto.randomUUID();
export function retryAfter(value, now = Date.now()) {
  if (!value) return 0;
  const seconds = Number(value);
  return Math.max(0, Number.isFinite(seconds) ? seconds * 1000 : Date.parse(value) - now) || 0;
}
export function recoverable(error) { return error instanceof EngineError && [0, 429, 502, 503, 504].includes(error.status); }

// Inquiry precedes any payment replay. Every POST uses the original identity and payload.
export async function initiateWithRecovery(adapter, orderId, key, payload, {deadline, onRecovery = () => {}, wait = sleep, random = Math.random} = {}) {
  if (!Number.isFinite(deadline)) throw new EngineError('ORIGINAL_DEADLINE_REQUIRED');
  for (let attempt = 0; attempt < 3; attempt++) {
    if (Date.now() >= deadline) throw new EngineError('PAYMENT_DEADLINE_REACHED');
    try { return await adapter.pay(orderId, key, payload); }
    catch (error) {
      if (!recoverable(error)) throw error;
      onRecovery('Checking the original payment before retrying');
      let existing;
      try { existing = await adapter.inquire(orderId, key); }
      catch { throw new EngineError('PAYMENT_INQUIRY_UNAVAILABLE', 0); }
      if (existing.payment_id) { onRecovery('Existing payment found · no second initiation'); return existing; }
      if (existing.state !== 'NOT_STARTED') throw new EngineError('PAYMENT_OUTCOME_UNKNOWN');
      if (attempt === 2) throw error;
      const delay = Math.max(error.retryAfterMs || 0, Math.min(1000, 100 * 2 ** attempt) * (.5 + random()));
      if (Date.now() + delay >= deadline) throw new EngineError('PAYMENT_RECOVERY_BUDGET_EXHAUSTED');
      await wait(delay);
    }
  }
}

export class LiveEngine {
  constructor({token, onRequest = () => {}}) { this.token = token; this.onRequest = onRequest; this.snapshots = new Map(); }
  async request(path, {method = 'GET', body, key, conditional = false} = {}) {
    const headers = {'X-Demo-Client':'encore'};
    if (this.token) headers.Authorization = `Bearer ${this.token}`;
    if (body) headers['Content-Type'] = 'application/json';
    if (key) headers['Idempotency-Key'] = key;
    const prior = this.snapshots.get(path);
    if (conditional && prior?.etag) headers['If-None-Match'] = prior.etag;
    let response;
    try { response = await fetch('/engine' + path, {method, headers, body:body ? JSON.stringify(body) : undefined, signal:AbortSignal.timeout(12000), cache:'no-store'}); }
    catch { this.onRequest(method, path, 0); throw new EngineError('NETWORK_RESPONSE_UNKNOWN'); }
    this.onRequest(method, path, response.status);
    if (response.status === 304 && prior) return prior.body;
    let value;
    try { value = await response.json(); } catch { throw new EngineError('INVALID_ENGINE_RESPONSE', response.status); }
    if (!response.ok) throw new EngineError(value.code || 'ENGINE_REQUEST_FAILED', response.status, retryAfter(response.headers.get('Retry-After')));
    if (conditional) this.snapshots.set(path,{body:value,etag:response.headers.get('ETag')});
    const poll = Number(response.headers.get('X-Poll-Interval-Ms'));
    if (poll > 0) this.pollMs = Math.min(30000, Math.max(1000, poll));
    return value;
  }
  events() { return this.request('/v1/events'); }
  layout(id) { return this.request(`/v1/events/${id}/layout`); }
  availability(id) { return this.request(`/v1/events/${id}/availability`,{conditional:true}); }
  reserve(event, seats, key) { return this.request('/v1/holds',{method:'POST',body:{event_id:event,seat_ids:seats},key}); }
  command(event,id) { return this.request(`/v1/reservation-commands/${event}/${id}`); }
  release(id) { return this.request(`/v1/holds/${id}`,{method:'DELETE'}); }
  pay(order,key,body) { return this.request(`/v1/orders/${order}/payments`,{method:'POST',body,key}); }
  inquire(order,key) { return this.request(`/v1/orders/${order}/payment-operation`,{key}); }
  order(id) { return this.request(`/v1/orders/${id}`); }
  metrics() { return this.request('/metrics'); }
}

const SAMPLE_KEY = 'encore.sample.v1';
export class SampleEngine {
  constructor({onRequest = () => {}, scenario = () => 'normal', storage = globalThis.sessionStorage, now = Date.now} = {}) {
    this.onRequest=onRequest; this.scenario=scenario; this.storage=storage; this.now=now;
    try { this.state=JSON.parse(storage?.getItem(SAMPLE_KEY) || 'null'); } catch { /* Reset invalid sample data. */ }
    this.state ||= {orders:{},keys:{},conflicts:[],version:1};
  }
  save() { this.storage?.setItem(SAMPLE_KEY,JSON.stringify(this.state)); }
  async observe(method,path,body,status=200) { await sleep(110); this.onRequest(method,path,status); return structuredClone(body); }
  async events() { return this.observe('GET','/v1/events',[
    {id:'sample-night-signals',name:'Night Signals',subtitle:'The live experience',venue:'Bangkok Arena',date:'24 October 2026',time:'19:00',currency:'THB'},
    {id:'sample-sunday-sessions',name:'Sunday Sessions',subtitle:'An evening in the city',venue:'Riverside Hall',date:'8 November 2026',time:'18:30',currency:'THB'}]); }
  async layout(event) { return this.observe('GET','/layout',{event_id:event,seats:Array.from({length:96},(_,i)=>({seat_id:String.fromCharCode(65+Math.floor(i/12))+(i%12+1),price:i<36?250000:i<72?180000:120000}))}); }
  seatStatus(event,seat,index) {
    for (const order of Object.values(this.state.orders)) if (order.event===event && order.seats.includes(seat)) {
      if (this.final(order)) return 'SOLD';
      if (order.expires>this.now()) return 'HELD';
    }
    return this.state.conflicts.includes(event+':'+seat) || [3,4,14,26,27,50,65,82].includes(index) ? 'SOLD' : 'AVAILABLE';
  }
  async availability(event) { return this.observe('GET','/availability',{event_id:event,version:this.state.version,seats:Array.from({length:96},(_,i)=>({seat_id:String.fromCharCode(65+Math.floor(i/12))+(i%12+1),status:this.seatStatus(event,String.fromCharCode(65+Math.floor(i/12))+(i%12+1),i)}))}); }
  async reserve(event,seats,key) {
    const fingerprint=JSON.stringify([event,seats]);
    if (this.state.keys[key]) {
      const old=this.state.keys[key];
      if (old.fingerprint!==fingerprint) throw new EngineError('IDEMPOTENCY_CONFLICT',409);
      return this.observe('POST','/v1/holds',this.hold(this.state.orders[old.order]),202);
    }
    if (this.scenario()==='conflict' && !this.state.conflictInjected) {
      this.state.conflicts.push(event+':'+seats[0]);this.state.conflictInjected=true;this.save();this.onRequest('POST','/v1/holds',409);throw new EngineError('SEAT_UNAVAILABLE',409);
    }
    if (seats.some(seat=>this.seatStatus(event,seat,(seat.charCodeAt(0)-65)*12+Number(seat.slice(1))-1)!=='AVAILABLE')) throw new EngineError('SEAT_UNAVAILABLE',409);
    const order={id:operationKey(),hold:operationKey(),command:operationKey(),event,seats,expires:this.now()+120000,durableAt:this.now()+850,created:this.now()};
    this.state.orders[order.id]=order;this.state.keys[key]={order:order.id,fingerprint};this.state.version++;this.save();
    return this.observe('POST','/v1/holds',this.hold(order),202);
  }
  hold(order) { return {hold_id:order.hold,order_id:order.id,command_id:order.command,expires_at:new Date(order.expires).toISOString(),persistence_status:this.now()>=order.durableAt?'DURABLE':'PENDING'}; }
  async command(event,id) { const order=Object.values(this.state.orders).find(o=>o.event===event&&o.command===id);if(!order)throw new EngineError('NOT_FOUND',404);return this.observe('GET','/command',this.hold(order)); }
  async release(id) { const order=Object.values(this.state.orders).find(o=>o.hold===id);if(order&&!order.payment){order.expires=0;this.save();}return this.observe('DELETE','/hold',{status:'released'}); }
  async pay(id,key,payload) {
    const order=this.state.orders[id];if(!order)throw new EngineError('NOT_FOUND',404);
    if(order.payment && (order.payment.key!==key||order.payment.payload!==JSON.stringify(payload)))throw new EngineError('IDEMPOTENCY_CONFLICT',409);
    if(!order.payment) {
      if(this.now()>=order.expires)throw new EngineError('HOLD_EXPIRED',409);
      order.payment={id:operationKey(),key,payload:JSON.stringify(payload),paidAt:this.now()+650,issuedAt:this.now()+1800,tickets:order.seats.map(seat=>({id:operationKey(),seat_id:seat}))};this.save();
      if(this.scenario()==='recover') {this.onRequest('POST','/payments',0);throw new EngineError('NETWORK_RESPONSE_UNKNOWN');}
    }
    return this.observe('POST','/payments',{payment_id:order.payment.id,order_id:id},202);
  }
  async inquire(id,key) { const order=this.state.orders[id];if(!order)throw new EngineError('NOT_FOUND',404);const p=order.payment;return this.observe('GET','/payment-operation',{state:p&&p.key===key?'PENDING':'NOT_STARTED',payment_id:p&&p.key===key?p.id:null}); }
  final(order) { return order.payment && this.now()>=order.payment.issuedAt; }
  async order(id) { const o=this.state.orders[id];if(!o)throw new EngineError('NOT_FOUND',404);return this.observe('GET','/orders',{id,order_id:id,status:this.final(o)?'FULFILLED':o.payment&&this.now()>=o.payment.paidAt?'PAID':o.expires<=this.now()?'EXPIRED':'PENDING',tickets:this.final(o)?o.payment.tickets:[]}); }
  async metrics() { return {sample:true}; }
  reset() { this.state={orders:{},keys:{},conflicts:[],version:1};this.save(); }
}
