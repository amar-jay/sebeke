import zenoh
session = zenoh.open(zenoh.Config())
# This should trigger the Rust 'push_to_cloudflare' function
session.put("sensors/temp", b"32.5")
