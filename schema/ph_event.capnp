@0xc0e7b4e3cefaf83e;

using TimePointInNs = UInt64;
# Nanoseconds since the epoch.

using Ph = Data;

using GroupName = Text;

struct PhEvent {
    # pH datapoint with an associated timestamp in milliseconds
    ph @0 :Ph;
    timestamp @1 :TimePointInNs;
    groupName @2: GroupName;
}
