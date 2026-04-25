import { decode, encode } from "cbor-x";

// this needs to be properly implemnented to persist data across worker instances and restarts. 
// for discovery of edge machines, we can use Cloudflare's KV storage or Durable Objects,

export interface Env {
  // Bind the KV namespace to the environment
  EDGE_MACHINES: KVNamespace;
  MULTIMEDIA_HUB: DurableObjectNamespace;
}

const DEDUPE_PREFIX = "dedupe:";
const DEDUPE_TTL_SECONDS = 60;
const MULTIMEDIA_HUB_INSTANCE = "global-multimedia-hub";

function dedupeKvKey(messageKey: string): string {
  return `${DEDUPE_PREFIX}${messageKey}`;
}

async function computeMessageKey(topic: string, data: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", data);
  const hashBytes = new Uint8Array(digest);
  let hashHex = "";

  for (const byte of hashBytes) {
    hashHex += byte.toString(16).padStart(2, "0");
  }

  return `${topic}:${hashHex}`;
}

interface PushPayload {
  machine_id: string;
  topic: string;
  data: Uint8Array;
}

interface BindPayload {
	machine_id: string;
	ingress_url: string; // The public URL of our tunnel that the worker should send requests to
	ttl_ms?: number; // Backward-compatible field name, interpreted as TTL in seconds
}

function normalizeBindTtl(ttlRaw: number | undefined): number {
  if (!ttlRaw || !Number.isFinite(ttlRaw)) {
    return 86400;
  }

  // Accept legacy millisecond-ish values by converting when value looks too large.
  const seconds = ttlRaw > 1_000_000 ? Math.floor(ttlRaw / 1000) : Math.floor(ttlRaw);
  return Math.max(60, seconds);
}

function isTelemetryTopic(topic: string): boolean {
  return topic.startsWith("telemetry");
}

function getMultimediaHub(env: Env): DurableObjectStub {
  const id = env.MULTIMEDIA_HUB.idFromName(MULTIMEDIA_HUB_INSTANCE);
  return env.MULTIMEDIA_HUB.get(id);
}

async function dedupeMessage(env: Env, topic: string, data: Uint8Array): Promise<boolean> {
  const messageKey = await computeMessageKey(topic, data);
  const messageKvKey = dedupeKvKey(messageKey);
  const seen = await env.EDGE_MACHINES.get(messageKvKey);
  if (seen !== null) {
    return true;
  }

  await env.EDGE_MACHINES.put(messageKvKey, "1", {
    expirationTtl: DEDUPE_TTL_SECONDS,
  });
  return false;
}

async function fanoutTelemetryOverHttp(env: Env, machineId: string, payload: PushPayload, ctx: ExecutionContext): Promise<void> {
  const pullPayload = [{
    topic: payload.topic,
    data: payload.data
  }];
  const encodedPull = encode(pullPayload);

  const machineList = await env.EDGE_MACHINES.list();
  const promises: Promise<any>[] = [];

  for (const key of machineList.keys) {
    if (key.name.startsWith(DEDUPE_PREFIX)) continue;
    if (key.name === machineId) continue;

    const ingressUrl = await env.EDGE_MACHINES.get(key.name);
    if (!ingressUrl) continue;

    const destUrl = `${ingressUrl}/pull`;
    const outFormData = new FormData();
    outFormData.append("payload", new Blob([encodedPull], { type: "application/cbor" }));

    promises.push(
      fetch(destUrl, {
        method: "POST",
        body: outFormData,
      })
        .then(async (resp) => {
          if (!resp.ok) {
            const body = await resp.text();
            console.error(`Ingress delivery failed ${resp.status} ${resp.statusText} for ${destUrl}: ${body}`);
          }
        })
        .catch((e) => console.error(`Failed to push to ${destUrl}:`, e))
    );
  }

  ctx.waitUntil(Promise.allSettled(promises));
}

async function fanoutMultimediaOverDurableObject(env: Env, machineId: string, payload: PushPayload): Promise<void> {
  const hub = getMultimediaHub(env);
  const body = encode(payload);
  const response = await hub.fetch("https://multimedia-hub/fanout", {
    method: "POST",
    headers: {
      "content-type": "application/cbor",
      "x-machine-id": machineId,
    },
    body,
  });

  if (!response.ok) {
    throw new Error(`multimedia fanout failed with status ${response.status}`);
  }
}

export  default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    console.log(`Worker received ${request.method} ${url.pathname}`);

    // --- BIND ENDPOINT ---
    if (request.method === "POST" && url.pathname === "/bind") {
      try {
        const body: BindPayload = await request.json();
        if (body.machine_id && body.ingress_url) {
          const ttlSeconds = normalizeBindTtl(body?.ttl_ms);
          // Store in KV. Keys expire after 24 hours just in case a node dies without unbinding.
          // The node should ideally re-bind periodically or on startup.
          await env.EDGE_MACHINES.put(body.machine_id, body.ingress_url, { expirationTtl: ttlSeconds });
          console.log(`Registered edge machine: ${body.machine_id} -> ${body.ingress_url}`);
          return new Response(JSON.stringify({ success: true }), { status: 200 });
        }
        return new Response("Missing machine_id or ingress_url", { status: 400 });
      } catch (e) {
        const msg = e instanceof Error ? e.message : "Unknown bind error";
        return new Response(`Bind failed: ${msg}`, { status: 400 });
      }
    }

    if (request.method === "GET" && url.pathname === "/ws") {
      const upgradeHeader = request.headers.get("Upgrade");
      if (upgradeHeader?.toLowerCase() !== "websocket") {
        return new Response("Expected websocket upgrade", { status: 426 });
      }
      return getMultimediaHub(env).fetch(request);
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
    if (request.method === "POST") {
      try {
        const formData = await request.formData();
        const machineId = formData.get("machine_id") as string;
        const payloadField = formData.get("payload");

        if (!machineId || !payloadField || !(payloadField instanceof Blob)) {
          return new Response("Missing valid machine_id or payload Blob", { status: 400 });
        }

        const payloadBuffer = await payloadField.arrayBuffer();
        const pushPayload = decode(new Uint8Array(payloadBuffer)) as PushPayload;

        if (isTelemetryTopic(pushPayload.topic)) {
          const alreadySeen = await dedupeMessage(env, pushPayload.topic, pushPayload.data);
          if (alreadySeen) {
            console.log(`Deduped repeated push for topic ${pushPayload.topic}`);
            return new Response("OK", { status: 200 });
          }
          await fanoutTelemetryOverHttp(env, machineId, pushPayload, ctx);
        } else {
          // Multimedia should flow over durable websocket channel.
          await fanoutMultimediaOverDurableObject(env, machineId, pushPayload);
        }

        return new Response("OK", { status: 200 });

      } catch (e) {
        console.error(`Broadcast failed for ${url.pathname}:`, e);
        return new Response("Internal Server Error", { status: 500 });
      }
    }

    return new Response("Not Found", { status: 404 });
  }
};

export class MultimediaHub {
  private readonly env: Env;
  private readonly sockets = new Map<WebSocket, string>();

  constructor(_state: DurableObjectState, env: Env) {
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get("Upgrade")?.toLowerCase();

    if (upgradeHeader === "websocket") {
      const machineId = url.searchParams.get("machine_id") || "unknown-machine";
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];

      server.accept();
      this.sockets.set(server, machineId);

      server.addEventListener("message", (event) => {
        void this.handleSocketMessage(server, event.data);
      });

      server.addEventListener("close", () => {
        this.sockets.delete(server);
      });

      server.addEventListener("error", () => {
        this.sockets.delete(server);
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    if (request.method === "POST" && url.pathname === "/fanout") {
      const senderMachineId = request.headers.get("x-machine-id") || "unknown-machine";
      const body = new Uint8Array(await request.arrayBuffer());
      const pushPayload = decode(body) as PushPayload;

      const alreadySeen = await dedupeMessage(this.env, pushPayload.topic, pushPayload.data);
      if (alreadySeen) {
        return new Response("OK", { status: 200 });
      }

      this.broadcastMultimedia(senderMachineId, pushPayload);
      return new Response("OK", { status: 200 });
    }

    return new Response("Not Found", { status: 404 });
  }

  private async handleSocketMessage(senderSocket: WebSocket, data: string | ArrayBuffer): Promise<void> {
    try {
      let raw: Uint8Array;
      if (data instanceof ArrayBuffer) {
        raw = new Uint8Array(data);
      } else {
        raw = new TextEncoder().encode(data);
      }

      const pushPayload = decode(raw) as PushPayload;
      const senderMachineId = this.sockets.get(senderSocket) || "unknown-machine";

      const alreadySeen = await dedupeMessage(this.env, pushPayload.topic, pushPayload.data);
      if (alreadySeen) {
        return;
      }

      this.broadcastMultimedia(senderMachineId, pushPayload);
    } catch (e) {
      console.error("Durable Object websocket message handling failed:", e);
    }
  }

  private broadcastMultimedia(senderMachineId: string, payload: PushPayload): void {
    const encodedPull = encode([{ topic: payload.topic, data: payload.data }]);

    for (const [socket, machineId] of this.sockets.entries()) {
      if (machineId === senderMachineId) {
        continue;
      }

      try {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(encodedPull);
        }
      } catch (e) {
        console.error("Durable Object multimedia broadcast failed:", e);
      }
    }
  }
}