#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import threading
import time
import random
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional, Set

from ..config.settings import settings

logger = logging.getLogger(__name__)


class Peer:
    def __init__(self, addr: str):
        self.addr = addr
        self.last_seen: float = 0.0
        self.is_alive: bool = True
        self.incarnation: int = 0

    def __repr__(self) -> str:
        return f"Peer(addr={self.addr}, alive={self.is_alive})"


class GossipMembership:
    def __init__(
        self,
        self_addr: str,
        interval: float = 1.0,
        failure_timeout: float = 5.0,
    ):
        self.self_addr = self_addr
        self.interval = max(0.5, float(interval))
        self.failure_timeout = max(1.0, float(failure_timeout))
        self.peers: Dict[str, Peer] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._callbacks: List[callable] = []

        # Add self to peers
        self.peers[self_addr] = Peer(self_addr)
        self.peers[self_addr].last_seen = time.time()

    def on_membership_change(self, callback: callable) -> None:
        self._callbacks.append(callback)

    def _notify_callbacks(self) -> None:
        for callback in self._callbacks:
            try:
                callback(self.get_alive_peers())
            except Exception as e:
                logger.error(f"Failed to notify callback: {e}")

    def add_peer(self, addr: str) -> None:
        with self._lock:
            if addr not in self.peers:
                self.peers[addr] = Peer(addr)
                self.peers[addr].last_seen = time.time()
                self._notify_callbacks()
                logger.info(f"Added new peer: {addr}")

    def remove_peer(self, addr: str) -> None:
        with self._lock:
            if addr in self.peers:
                del self.peers[addr]
                self._notify_callbacks()
                logger.info(f"Removed peer: {addr}")

    def update_peer(self, addr: str, is_alive: bool, incarnation: Optional[int] = None) -> None:
        with self._lock:
            if addr not in self.peers:
                self.peers[addr] = Peer(addr)

            peer = self.peers[addr]
            if incarnation is not None and incarnation > peer.incarnation:
                peer.incarnation = incarnation
                peer.is_alive = is_alive
                peer.last_seen = time.time()
                self._notify_callbacks()
                logger.debug(f"Updated peer {addr}: alive={is_alive}, incarnation={incarnation}")
            elif incarnation is None:
                peer.is_alive = is_alive
                peer.last_seen = time.time()
                self._notify_callbacks()
                logger.debug(f"Updated peer {addr}: alive={is_alive}")

    def get_alive_peers(self) -> List[str]:
        with self._lock:
            now = time.time()
            alive = []
            for peer in self.peers.values():
                if peer.addr == self.self_addr:
                    alive.append(peer.addr)
                elif peer.is_alive and (now - peer.last_seen) < self.failure_timeout:
                    alive.append(peer.addr)
                elif peer.is_alive:
                    peer.is_alive = False
                    self._notify_callbacks()
                    logger.warning(f"Marked peer {peer.addr} as dead due to timeout")
            return alive

    def get_all_peers(self) -> List[str]:
        with self._lock:
            return list(self.peers.keys())

    def _get_random_peers(self, count: int = 3) -> List[str]:
        with self._lock:
            available = [p.addr for p in self.peers.values() if p.addr != self.self_addr]
            if len(available) <= count:
                return available
            return random.sample(available, count)

    def _gossip_with_peer(self, peer_addr: str) -> None:
        try:
            url = f"http://{peer_addr}/gossip/membership"
            with self._lock:
                our_state = {
                    "addr": self.self_addr,
                    "peers": {
                        addr: {
                            "alive": p.is_alive,
                            "incarnation": p.incarnation,
                            "last_seen": p.last_seen
                        }
                        for addr, p in self.peers.items()
                    }
                }

            req = urllib.request.Request(
                url,
                data=json.dumps(our_state).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            # Update our state from peer's state
            if "peers" in data:
                with self._lock:
                    for addr, peer_data in data["peers"].items():
                        if addr == self.self_addr:
                            continue
                        if addr not in self.peers:
                            new_peer = Peer(addr)
                            new_peer.is_alive = peer_data.get("alive", True)
                            new_peer.incarnation = peer_data.get("incarnation", 0)
                            new_peer.last_seen = time.time()
                            self.peers[addr] = new_peer
                        else:
                            their_incarnation = peer_data.get("incarnation", 0)
                            if their_incarnation > self.peers[addr].incarnation:
                                self.peers[addr].incarnation = their_incarnation
                                self.peers[addr].is_alive = peer_data.get("alive", True)
                                self.peers[addr].last_seen = time.time()
                            elif their_incarnation == self.peers[addr].incarnation:
                                self.peers[addr].last_seen = time.time()

                self._notify_callbacks()

            self.update_peer(peer_addr, True)
        except Exception as e:
            logger.debug(f"Failed to gossip with {peer_addr}: {e}")
            self.update_peer(peer_addr, False)

    def _run(self) -> None:
        logger.info(f"Gossip membership running at {self.self_addr}")
        while not self._stop_event.is_set():
            try:
                peers_to_gossip = self._get_random_peers(3)
                for peer in peers_to_gossip:
                    if self._stop_event.is_set():
                        break
                    self._gossip_with_peer(peer)
            except Exception as e:
                logger.error(f"Gossip loop error: {e}")
            self._stop_event.wait(self.interval)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is not None:
            self._stop_event.set()
            self._thread.join(timeout=5.0)


# Global gossip membership instance
_gossip_instance: Optional[GossipMembership] = None


def get_gossip_membership() -> Optional[GossipMembership]:
    return _gossip_instance


def initialize_gossip(
    self_addr: str,
    interval: float = 1.0,
    failure_timeout: float = 5.0,
    seed_peers: Optional[List[str]] = None,
) -> GossipMembership:
    global _gossip_instance
    if _gossip_instance is not None:
        return _gossip_instance

    _gossip_instance = GossipMembership(
        self_addr=self_addr,
        interval=interval,
        failure_timeout=failure_timeout,
    )

    # Add seed peers
    if seed_peers:
        for peer in seed_peers:
            if peer != self_addr:
                _gossip_instance.add_peer(peer)

    return _gossip_instance
