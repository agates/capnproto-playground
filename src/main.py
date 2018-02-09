#!/usr/bin/env python3

from datetime import datetime, timezone
import signal
import socket
import sys
import time
from threading import Lock

from zeroconf import ServiceBrowser, Zeroconf

from schema.struct_handler_info import StructHandlerInfo
from schema.ph_event import PhEvent


def extract_data_pathway(info):
    try:
        return StructHandlerInfo.loads(info.properties[b"struct-handler-info"])
    except KeyError:
        raise KeyError(
            "'struct-handler-info' does not exist in service properties on '{0}', {1}:{2}, {3}".format(
                info.name, socket.inet_ntoa(info.address), info.server, info.port
            ))


class Browser:
    def __init__(self, capnproto_struct):
        self.capnproto_struct = capnproto_struct
        self.struct_name = bytes(capnproto_struct.__name__, "UTF-8")

        # Lock is required because zeroconf is threaded
        self.endpoints = {}
        self.endpoints_lock = Lock()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.sock.close()

    def add_endpoint(self, name, endpoint):
        with self.endpoints_lock:
            self.endpoints[name] = endpoint

    def remove_endpoint(self, name):
        with self.endpoints_lock:
            del self.endpoints[name]

    def remove_all_endpoints(self):
        with self.endpoints_lock:
            self.endpoints = {}

    def add_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        data_pathway = extract_data_pathway(info)

        if data_pathway.struct_name == self.struct_name:
            self.add_endpoint(name, (socket.inet_ntoa(info.address), info.port))

    def remove_service(self, zeroconf, type, name):
        self.remove_endpoint(name)

    def send_struct(self, capnproto_object):
        if isinstance(capnproto_object, self.capnproto_struct):
            data = capnproto_object.dumps()
        else:
            raise ValueError("Must send a capnpy struct of {0}".format(self.struct_name))

        with self.endpoints_lock:
            endpoints = self.endpoints.values()

        for endpoint in endpoints:
            self.sock.sendto(data, endpoint)


def current_timestamp():
    # returns floating point timestamp in seconds
    return datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()


def current_timestamp_nanoseconds():
    return current_timestamp() * 10 ** 9


signal.signal(signal.SIGINT, signal.default_int_handler)

zeroconf = Zeroconf()
phevent_browser = Browser(PhEvent)
zeroconf_browser = ServiceBrowser(zeroconf, "_data-pathway._udp.local.", phevent_browser)

try:
    while True:
        ph_event = PhEvent(ph=7.0, timestamp=int(round(current_timestamp_nanoseconds())))

        print(ph_event)

        phevent_browser.send_struct(ph_event)

        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    zeroconf_browser.cancel()
    zeroconf.close()
    phevent_browser.close()
    sys.exit()
