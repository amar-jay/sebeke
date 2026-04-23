import { decode } from "cbor-x";

export interface Env {}

interface PushPayload {
machine_id: string;
topic: string;
data: Uint8Array;
}

const TEST_STORE = new Map<string, string>();
const REGISTERED_EDGE_MACHINES = new Set<string>();

export default {
async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
const url = new URL(request.url);

if (request.method === "GET" && url.pathname === "/data") {
const topic = url.searchParams.get("topic");
if (!topic) return new Response("Missing ?topic= query param", { status: 400 });

const data = TEST_STORE.get(topic);
return new Response(JSON.stringify({ topic, data: data || null }), {
status: 200, headers: { "content-type": "application/json" }
});
}

if (request.method === "POST" && url.pathname === "/bind") {
try {
const body: any = await request.json();
if (body.machine_id) {
REGISTERED_EDGE_MACHINES.add(body.machine_id);
return new Response(JSON.stringify({ success: true, bound: body.machine_id }), { status: 200 });
}
return new Response("Missing machine_id", { status: 400 });
} catch (e) {
return new Response("Invalid JSON", { status: 400 });
}
}

if (request.method === "POST" && url.pathname === "/unbind") {
return new Response(JSON.stringify({ success: true }), { status: 200 });
}

if (request.method === "POST" && url.pathname === "/") {
try {
const contentType = request.headers.get("content-type") || "";

if (!contentType.includes("multipart/form-data")) {
    return new Response("Expected multipart/form-data for telemetry", { status: 415 });
                }

const formData = await request.formData();
const machineId = formData.get("machine_id") as string;
const payloadField = formData.get("payload");

if (!machineId || !payloadField) {
return new Response("Missing machine_id or payload", { status: 400 });
}

let payloadBuffer: ArrayBuffer;
if (payloadField instanceof Blob) {
payloadBuffer = await payloadField.arrayBuffer();
} else if (typeof payloadField === "string") {
payloadBuffer = new TextEncoder().encode(payloadField).buffer;
} else {
return new Response("Invalid payload type", { status: 400 });
}

const pushPayload = decode(new Uint8Array(payloadBuffer)) as PushPayload;
const innerText = new TextDecoder().decode(pushPayload.data);

TEST_STORE.set(pushPayload.topic, innerText);

return new Response(JSON.stringify({ 
success: true, 
received: pushPayload.topic, 
machine_id: pushPayload.machine_id 
}), {
status: 200, headers: { "content-type": "application/json" }
});

} catch (err: any) {
return new Response(`Server Error: ${err.message}`, { status: 500 });
}
}

return new Response("Sebeke Cloudflare Worker is running", { status: 200 });
},
};
