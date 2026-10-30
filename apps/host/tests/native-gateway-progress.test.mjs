import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {nativeGateway} from '../server/native-gateway.mjs';
const listen=server=>new Promise(r=>server.listen(0,'127.0.0.1',()=>r(server.address().port)));
const close=server=>new Promise(r=>{server.closeAllConnections();server.close(r);});
/* "return_progress": true is llama.cpp's flag; the engine answers it with prompt_progress chunks while it reads the prompt.
   The gateway must forward the flag and hand each chunk on as it comes, not after the read: that is the point of it. */
test('native gateway forwards return_progress and passes prompt_progress chunks through unchanged and at once',{timeout:10000},async()=>{
 const progress=[{total:141414,cache:4096,processed:10000,time_ms:2500},{total:141414,cache:4096,processed:15888,time_ms:5000}];
 const frames=progress.map(p=>`data: ${JSON.stringify({id:'chatcmpl-1',object:'chat.completion.chunk',created:1,model:'native-a',choices:[{index:0,delta:{},finish_reason:null}],prompt_progress:p})}\n\n`);
 const tail='data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":141414,"completion_tokens":1}}\n\ndata: [DONE]\n\n';
 let received=null,seen=null;const clientHas=new Promise(r=>{seen=r;});
 const engine=http.createServer(async(req,res)=>{let text='';for await(const chunk of req)text+=chunk;received=JSON.parse(text);
  res.writeHead(200,{'content-type':'text/event-stream'});res.write(': keep-alive\n\n');
  for(const frame of frames){res.write(frame);await clientHas;}   // the engine goes on only once the client has the first chunk
  res.end(tail);
 });const port=await listen(engine);
 const instance={id:'first',apiModelId:'native-a',port,key:'private-a',gpus:[0]};
 const serving={open:value=>({...value,id:'0'}),mark:()=>{},finish:()=>{}};
 const gateway=http.createServer(async(req,res)=>{let text='';for await(const chunk of req)text+=chunk;await nativeGateway({req,res,path:req.url,body:JSON.parse(text),control:{resolveInstance:()=>instance},serving});});const publicPort=await listen(gateway);
 try{
  const result=await fetch(`http://127.0.0.1:${publicPort}/v1/chat/completions`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({model:'native-a',stream:true,return_progress:true,messages:[{role:'user',content:'Hello'}]})});
  assert.equal(result.status,200);assert.equal(result.headers.get('x-accel-buffering'),'no');
  const reader=result.body.getReader(),decoder=new TextDecoder();let text='';
  while(!text.includes(frames[0])){const {value,done}=await reader.read();assert.equal(done,false,'the stream ended before the first progress chunk');text+=decoder.decode(value,{stream:true});}
  seen();   // before this the engine has not finished: a gateway that buffered the stream would never get here
  for(;;){const {value,done}=await reader.read();if(done)break;text+=decoder.decode(value,{stream:true});}
  assert.equal(received.return_progress,true);
  assert.equal(text,': keep-alive\n\n'+frames.join('')+tail,'the engine\'s bytes, comments and progress chunks included, unchanged');
 }finally{await close(gateway);await close(engine);}
});
