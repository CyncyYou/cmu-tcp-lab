#!/usr/bin/env python3
"""Reproduce CP2 slow-start measurement the way the autograder likely does it:
act as INITIATOR, handshake, trigger the student's testing_server to send, then
ACK each received segment. Run with different advertised-window values to see
which one reproduces the autograder's 1375,2750,5500,5375 pattern.

Run inside the client container:
    sudo -E python3 tests/test_cp2_repro.py [adv_window]
"""

import sys
import time

from scapy.all import sr1, sniff, send, Raw
from fabric import Connection

from common import (
    CMUTCP, ACK_MASK, SYN_MASK, IFNAME, TESTING_HOST_IP, ip, udp,
)

START_SERVER = (
    "tmux new -s pytest_server -d "
    "'/vagrant/project-2_15-441/tests/testing_server 2> /tmp/cwnd_srv.log'"
)
STOP_SERVER = "tmux kill-session -t pytest_server"

ADV = int(sys.argv[1]) if len(sys.argv) > 1 else 60000


def handshake():
    syn = ip / udp / CMUTCP(plen=25, seq_num=1000, flags=SYN_MASK,
                            advertised_window=ADV)
    synack = sr1(syn, timeout=3, iface=IFNAME)
    assert synack is not None and synack[CMUTCP].flags == (SYN_MASK | ACK_MASK)
    server_isn = synack[CMUTCP].seq_num
    send(ip / udp / CMUTCP(plen=25, seq_num=1001, ack_num=server_isn + 1,
                           flags=ACK_MASK, advertised_window=ADV), iface=IFNAME)
    return 1001, server_isn + 1


def trigger(my_seq, srv_ack):
    send(ip / udp / CMUTCP(plen=26, seq_num=my_seq, ack_num=srv_ack,
                           flags=ACK_MASK, advertised_window=ADV) / Raw(load="x"),
         iface=IFNAME)


def measure(srv_ack, duration=6.0):
    state = {"ack": srv_ack}
    events = []

    def on_pkt(p):
        payload = p[CMUTCP].plen - p[CMUTCP].hlen
        end = p[CMUTCP].seq_num + payload
        if end > state["ack"]:
            state["ack"] = end
        send(ip / udp / CMUTCP(plen=25, seq_num=5000, ack_num=state["ack"],
                               flags=ACK_MASK, advertised_window=ADV),
             iface=IFNAME, verbose=0)
        events.append((time.time(), payload))

    sniff(iface=IFNAME, timeout=duration,
          lfilter=lambda p: CMUTCP in p and p[CMUTCP].plen > 25, prn=on_pkt)

    rounds, cur, last = [], 0, None
    for t, payload in events:
        if last is not None and (t - last) > 0.05:
            rounds.append(cur)
            cur = 0
        cur += payload
        last = t
    if cur:
        rounds.append(cur)
    return rounds


def main():
    print("ADV =", ADV)
    with Connection(host=TESTING_HOST_IP, user="vagrant",
                    connect_kwargs={"password": "vagrant"}) as conn:
        try:
            conn.run(START_SERVER)
            conn.run("tmux has-session -t pytest_server")
            time.sleep(0.5)
            my_seq, srv_ack = handshake()
            trigger(my_seq, srv_ack)
            print("FLIGHTS:", measure(srv_ack))
        finally:
            try:
                conn.run(STOP_SERVER)
            except Exception:
                pass


if __name__ == "__main__":
    main()
