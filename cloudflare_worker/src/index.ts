import { decode, encode } from "cbor-x";

// this needs to be properly implemnented to persist data across worker instances and restarts. 
// for discovery of edge machines, we can use Cloudflare's KV storage or Durable Objects,

export interface Env {
  // Bind the KV namespace to the environment
  EDGE_MACHINES: KVNamespace;
}

interface PushPayload {
  machine_id: string;
  topic: string;
  data: Uint8Array;
}

interface BindPayload {
	machine_id: string;
	ingress_url: string; // The public URL of our tunnel that the worker should send requests to
	ttl_ms?: number; // Optional TTL for this binding, if the worker supports it
}

export  default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // --- BIND ENDPOINT ---
    if (request.method === "POST" && url.pathname === "/bind") {
      try {
        const body: BindPayload = await request.json();
        if (body.machine_id && body.ingress_url) {
          // Store in KV. Keys expire after 24 hours just in case a node dies without unbinding.
          // The node should ideally re-bind periodically or on startup.
          await env.EDGE_MACHINES.put(body.machine_id, body.ingress_url, { expirationTtl: body?.ttl_ms || 86400 });
          console.log(`Registered edge machine: ${body.machine_id} -> ${body.ingress_url}`);
          return new Response(JSON.stringify({ success: true }), { status: 200 });
        }
        return new Response("Missing machine_id or ingress_url", { status: 400 });
      } catch (e) {
        return new Response("Invalid JSON", { status: 400 });
      }
    }

    // --- UNBIND ENDPOINT ---
    if (request.method === "POST" && url.pathname === "/unbind") {
      try {
        const body: any = await request.json();
        if (body.machine_id) {
          await env.EDGE_MACHINES.delete(body.machine_id);
          console.log(`Unregistered edge machine: ${body.machine_id}`);
        }
      } catch (e) {}
      return new Response(JSON.stringify({ success: true }), { status: 200 });
    }

    // --- BROADCAST / PUSH ENDPOINT ---
    if (request.method === "POST" && url.pathname === "/") {
      try {
        const formData = await request.formData();
        const machineId = formData.get("machine_id") as string;
        const payloadField = formData.get("payload");

        if (!machineId || !payloadField || !(payloadField instanceof Blob)) {
          return new Response("Missing valid machine_id or payload Blob", { status: 400 });
        }

        const payloadBuffer = await payloadField.arrayBuffer();
        const pushPayload = decode(new Uint8Array(payloadBuffer)) as PushPayload;

        const pullPayload = [{
            topic: pushPayload.topic,
            data: pushPayload.data 
        }];
        const encodedPull = encode(pullPayload);

        // Fetch ALL registered machines from KV
        const machineList = await env.EDGE_MACHINES.list();
        const promises: Promise<any>[] = [];
				console.log("machineList:", machineList.keys.map(k => k.name));

        for (const key of machineList.keys) {
          // Don't send back to the initiator
          if (key.name === machineId) continue;

          // Get the actual ingress URL for this machine
          const ingressUrl = await env.EDGE_MACHINES.get(key.name);
          if (!ingressUrl) continue;

          // Note: Change this to match whatever route your Axum router expects
          // Currently, your code maps `/bind` to `handle_pull_request`. 
          // You should probably change that route in Rust to `/pull`!
          const destUrl = `${ingressUrl}/pull`; 

          const outFormData = new FormData();
          outFormData.append("payload", new Blob([encodedPull], { type: "application/cbor" }));

          console.log(`Broadcasting to ${key.name} at ${destUrl}`);
          
          promises.push(
            fetch(destUrl, {
              method: 'POST',
              body: outFormData,
            }).catch(e => console.error(`Failed to push to ${destUrl}:`, e))
          );
        }

        // Wait for all broadcasts to fire (Execution Context waitUntil prevents the worker from dying prematurely)
        ctx.waitUntil(Promise.allSettled(promises));

        return new Response("OK", { status: 200 });

      } catch (e) {
        console.error("Broadcast failed:", e);
        return new Response("Internal Server Error", { status: 500 });
      }
    }

    return new Response("Not Found", { status: 404 });
  }
};