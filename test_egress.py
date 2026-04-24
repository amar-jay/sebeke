import zenoh

session = zenoh.open(zenoh.Config())
# Subscribe to everything
sub = session.declare_subscriber("**", lambda sample: 
    print(f">> Outbound to Zenoh: [{sample.key_expr}] : {sample.payload.to_bytes()}")
)

print("Listening for Zenoh messages... Press Ctrl+C to stop.")
import time
while True:
    time.sleep(1)
