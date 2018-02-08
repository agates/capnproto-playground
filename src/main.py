#!/usr/bin/env python3

from datetime import datetime, timezone
import signal
import socket
import sys
import time
from threading import Lock

from zeroconf import ServiceBrowser, Zeroconf

from schema.data_pathway import DataPathway
from schema.ph_event import PhEvent


def extract_data_pathway(info):
    try:
        return DataPathway.loads(info.properties[b'data-pathway'])
    except KeyError:
        raise KeyError(
            "'data-pathway' does not exist in service properties on '{0}', {1}:{2}, {3}".format(
                info.name, socket.inet_ntoa(info.address), info.server, info.port
            ))


class Browser:
    def __init__(self, capnproto_struct):
        self.capnproto_struct = capnproto_struct
        self.struct_name = bytes(capnproto_struct.__name__, "UTF-8")

        # Keep an internal set for uniqueness, tuple for faster iteration
        # Lock is required because zeroconf is threaded
        self._endpoints = tuple()
        self._endpoints_dict = {}
        self._endpoints_lock = Lock()

    @property
    def endpoints(self):
        with self._endpoints_lock:
            return self._endpoints

    def add_endpoint(self, name, e):
        with self._endpoints_lock:
            self._endpoints_dict[name] = e
            self.flatten_endpoints()

    def remove_endpoint(self, name):
        with self._endpoints_lock:
            del self._endpoints_dict[name]
            self.flatten_endpoints()

    def flatten_endpoints(self):
        self._endpoints = tuple(self._endpoints_dict.values())

    def add_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)

        data_pathway = extract_data_pathway(info)

        if data_pathway.struct_name == self.struct_name:
            self.add_endpoint(name, (socket.inet_ntoa(info.address), info.port))

    def remove_service(self, zeroconf, type, name):
        self.remove_endpoint(name)


def current_timestamp():
    # returns floating point timestamp in seconds
    return datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()


def current_timestamp_nanoseconds():
    return current_timestamp() * 10 ** 9


signal.signal(signal.SIGINT, signal.default_int_handler)

zeroconf = Zeroconf()
phevent_browser = Browser(PhEvent)
zeroconf_browser = ServiceBrowser(zeroconf, "_data-pathway._udp.local.", phevent_browser)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

try:
    while True:
        ph_event = PhEvent(ph=7.0, timestamp=int(round(current_timestamp_nanoseconds())))

        print(ph_event)
        ph_message = ph_event.dumps()

        for endpoint in phevent_browser.endpoints:
            r = sock.sendto(ph_message, endpoint)

        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    zeroconf_browser.cancel()
    zeroconf.close()
    sock.close()
    sys.exit()
