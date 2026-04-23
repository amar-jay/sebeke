import { decode } from "cbor-x";

export interface Env {}

interface PushPayload {
machine_id: string;
topic: string;
data: Uint8Array;
}

// Temporary in-memory store for testing POST -> GET
// Note: In production, use Cloudflare KV, D1, or Durable Objects
const TEST_STORE = new Map<string, string>();

export default {
async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
const url = new URL(request.url);

// Added GET route to retrieve posted data for testing
if (request.method === "GET" && url.pathname === "/data") {
const topic = url.searchParams.get("topic");
if (!topic) {
return new Response("Missing ?topic= query param", { status: 400 });
}

const data = TEST_STORE.get(topic);
return new Response(JSON.stringify({ topic, data: data || null }), {
status: 200,
headers: { 
"content-type": "application/json",
"access-control-allow-origin": "*"
}
});
}

if (request.method === "POST") {
try {
const contentType = request.headers.get("content-type") || "";

if (contentType.includes("multipart/form-data")) {
const formData = await request.formData();
const machineId = formData.get("machine_id") as string;
const payloadField = formData.get("payload");

if (!machineId || !payloadField) {
return new Response("Missing machine_id or payload", { status: 400 });
}

let payloadBuffer: ArrayBufferLike;
if (payloadField instanceof Blob) {
payloadBuffer = await payloadField.arrayBuffer();
} else if (typeof payloadField === "string") {
payloadBuffer = new TextEncoder().encode(payloadField).buffer;
} else {
return new Response("Invalid payload type", { status: 400 });
}

const pushPayload = decode(new Uint8Array(payloadBuffer)) as PushPayload;
const innerText = new TextDecoder().decode(pushPayload.data);

// Store it in the V8 isolate's global memory Map
TEST_STORE.set(pushPayload.topic, innerText);

return new Response(JSON.stringify({ 
success: true, 
received: pushPayload.topic, 
machine_id: pushPayload.machine_id 
}), {
status: 200,
headers: { 
"content-type": "application/json",
"access-control-allow-origin": "*"
}
});
}

return new Response("Expected multipart/form-data", { status: 415 });

} catch (err: any) {
return new Response(`Server Error: ${err.message}`, { status: 500 });
}
}

return new Response("Sebeke Cloudflare Worker is running", { status: 200 });
},
};
