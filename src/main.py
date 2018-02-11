#!/usr/bin/env python3

from datetime import datetime, timezone
import signal
import socket
import sys
from threading import Lock
import time

from zeroconf import ServiceBrowser, Zeroconf

from atlasi2c import AtlasI2c
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

device = AtlasI2c()
zeroconf = Zeroconf()
phevent_browser = Browser(PhEvent)
zeroconf_browser = ServiceBrowser(zeroconf, "_data-pathway._udp.local.", phevent_browser)

try:
    # get the information of the board you're polling
    info = device.query("I").split(",")[1]
    print("Polling {} sensor every {} seconds, press ctrl-c "
          "to stop polling".
          format(info, device.long_timeout))
    while True:
        # Query the i2c device, convert the string response to a float
        device.write("R")
        time.sleep(device.long_timeout)

        ph_bytes = device.read_binary()

        ph_event = PhEvent(ph=ph_bytes, timestamp=int(round(current_timestamp_nanoseconds())),
                           group_name="ph-probe-2018-02-11")

        phevent_browser.send_struct(ph_event)

        print(ph_event)
except Exception as e:
    print(e)
except KeyboardInterrupt:
    print("Continuous polling stopped")
finally:
    zeroconf_browser.cancel()
    zeroconf.close()
    phevent_browser.close()
    device.close()
    sys.exit()
