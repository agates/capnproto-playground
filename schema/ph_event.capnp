@0xc0e7b4e3cefaf83e;

using TimePointInNs = UInt64;
# Nanoseconds since the epoch.

using Ph = Float32;

struct PhEvent {
    # pH datapoint with an associated timestamp in milliseconds
    ph @0 :Ph;
    timestamp @1 :TimePointInNs;
}
