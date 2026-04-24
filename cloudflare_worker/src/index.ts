import { decode, encode } from "cbor-x";

export interface Env {}

interface PushPayload {
  machine_id: string;
  topic: string;
  data: Uint8Array;
}

const TEST_STORE = new Map<string, string>();
const REGISTERED_EDGE_MACHINES = new Map<string, string>();

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
        if (body.machine_id && body.ingress_url) {
          REGISTERED_EDGE_MACHINES.set(body.machine_id, body.ingress_url);
          return new Response(JSON.stringify({ success: true, bound: body.machine_id, ingress_url: body.ingress_url }), { status: 200 });
        }
        return new Response("Missing machine_id or ingress_url", { status: 400 });
      } catch (e) {
        return new Response("Invalid JSON", { status: 400 });
      }
    }

    if (request.method === "POST" && url.pathname === "/unbind") {
      try {
        const body: any = await request.json();
        if (body.machine_id) {
          REGISTERED_EDGE_MACHINES.delete(body.machine_id);
        }
      } catch(e) {}
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

        // Broadcast to all other registered edge machines
        const promises: Promise<any>[] = [];
        const pullPayload = [
          {
            topic: pushPayload.topic,
            data: pushPayload.data // Uint8Array
          }
        ];
        // CBOR encode the array
        const encodedPull = encode(pullPayload);

        for (const [id, ingressUrl] of REGISTERED_EDGE_MACHINES.entries()) {
          // Do not send the event back to the initiator
          if (id === pushPayload.machine_id) continue;

          const outFormData = new FormData();
          outFormData.append("payload", new Blob([encodedPull], { type: "application/cbor" }));

          // The destination is the ingress server which by default listens on /bind route
          const destUrl = `${ingressUrl}/bind`;

          promises.push(
            fetch(destUrl, {
              method: 'POST',
              body: outFormData,
            }).catch(e => console.error(`Failed to push to ${destUrl}:`, e))
          );
        }

        // Run network requests in the background
        if (promises.length > 0) {
          ctx.waitUntil(Promise.all(promises));
        }

        return new Response(JSON.stringify({ 
          success: true, 
          received: pushPayload.topic, 
          machine_id: pushPayload.machine_id,
          broadcast_to: promises.length
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
