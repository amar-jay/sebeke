import { encode } from "cbor-x";

const WORKER_URL = "https://cloudflare.abdelmanan-abdelrahman03.workers.dev";

async function runTest() {
  console.log(`[1/3] Testing Health Check...`);
  const healthRes = await fetch(WORKER_URL);
  const healthText = await healthRes.text();
  console.log(`Status: ${healthRes.status} | Response: ${healthText}\n`);

  console.log(`[2/3] Testing CBOR Multipart POST...`);
  const innerData = new TextEncoder().encode("Hello from Sebeke Test Script, testing GET request!");
  const pushPayload = {
    machine_id: "test-machine-99",
    topic: "sensors/imu/1",
    data: innerData,
  };

  const formData = new FormData();
  formData.append("machine_id", "test-machine-99");
  formData.append("payload", new Blob([encode(pushPayload)]), "payload.cbor");

  try {
    const postRes = await fetch(WORKER_URL, {
      method: "POST",
      body: formData,
    });

    if (postRes.ok) {
      const postJson = await postRes.json();
      console.log(`Status: ${postRes.status} | Success! Worker decoded payload:`);
      console.log(postJson);
    } else {
      console.log(`Status: ${postRes.status} | Failed! Worker responded:`);
      console.log(await postRes.text());
    }
  } catch (err) {
    console.error("Error making POST request:", err);
  }
  console.log('\n');

  console.log(`[3/3] Testing GET Data...`);
  const getRes = await fetch(`${WORKER_URL}/data?topic=sensors/imu/1`);
  if (getRes.ok) {
    const getJson = await getRes.json();
    console.log(`Status: ${getRes.status} | Retrieved:`);
    console.log(getJson);
  } else {
    console.log(`Status: ${getRes.status} | GET Failed! worker responded:`);
    console.log(await getRes.text());
  }
}

runTest();
