import cbor2
import requests

url = "http://localhost:8787/bind"

# ----------------------------
# This MUST be Vec<PullPayload>
# ----------------------------
payloads = [
    {
        "topic": "cloud/outbound/test",
        "data": b"hello world"
    },
    {
        "topic": "cloud/outbound/test2",
        "data": b"\x01\x02\x03\x04"
    }
]

# CBOR encode (IMPORTANT: top-level is a list)
cbor_bytes = cbor2.dumps(payloads)

files = {
    "payload": (
        "payload.cbor",
        cbor_bytes,
        "application/cbor"
    )
}

resp = requests.post(url, files=files)

print("status:", resp.status_code)
print(resp.text)
