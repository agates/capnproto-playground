#!/usr/bin/env python3

from datetime import datetime, timezone
import pprint
import socket
import time

from zeroconf import ServiceBrowser, Zeroconf

from schema.ph_event import PhEvent


class PhEventCapnprotoListener:
    def __init__(self):
        self.endpoints = []

    def remove_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        pprint.pprint(info)
        if info.properties[b'type'] == b'PhEvent':
            self.endpoints.remove((socket.inet_ntoa(info.address), info.port))

    def add_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)

        pprint.pprint(info)
        if info.properties[b'type'] == b'PhEvent':
            self.endpoints.append((socket.inet_ntoa(info.address), info.port))


zeroconf = Zeroconf()
listener = PhEventCapnprotoListener()
browser = ServiceBrowser(zeroconf, "_capnproto._udp.local.", listener)


def current_timestamp():
    # returns floating point timestamp in seconds
    return datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()


def current_timestamp_nanoseconds():
    return current_timestamp() * 10 ** 9


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

try:
    while True:
        ph_event = PhEvent(ph=7.0, timestamp=int(round(current_timestamp_nanoseconds())))

        print(ph_event)
        ph_message = ph_event.dumps()

        for endpoint in listener.endpoints:
            r = sock.sendto(ph_message, endpoint)

        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    sock.close()
    zeroconf.close()
