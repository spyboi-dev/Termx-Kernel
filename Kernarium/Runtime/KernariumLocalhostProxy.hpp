// SPDX-License-Identifier: GPL-2.0-or-later
#ifndef KERNARIUM_LOCALHOST_PROXY_HPP
#define KERNARIUM_LOCALHOST_PROXY_HPP

#include "KernariumRuntime.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <vector>

namespace kernarium {

// The host half of the localhost proxy (the guest half is
// tools/localhost-bridge/tx-vsock-bridge.c).
//
// A service listening on 127.0.0.1:PORT inside the guest is not
// reachable from the host: the guest sits behind slirp NAT, outbound
// only, and the runtime's public API has no port-forward call. The
// runtime does expose a vsock stream to the guest, so this proxy
// listens on the host's 127.0.0.1:PORT for each forwarded port and
// forwards each connection over vsock to the guest bridge (vsock
// 5999), which forwards it to guest 127.0.0.1:PORT. The result is
// that a guest localhost service answers at the host's localhost,
// the way it would on a native OS, for any port, any tool, any agent.
//
// The forwarded set is the union of two sources. The configured
// ports (the application's manual list) are always forwarded, even
// before anything listens in the guest. And the proxy polls the
// bridge once a second for the guest's listening TCP ports (a port-0
// control request; the bridge scans the guest's LISTEN sockets) and
// forwards those too, so any tool or agent that starts listening in
// the guest is reachable at the same 127.0.0.1:PORT on the host
// within about a second, with no configuration. When a guest service
// stops listening, its auto-forwarded port is dropped again. The
// configured list is the manual override; the polled list is the
// auto-mirror.
//
// The proxy uses only the runtime API's connect_vsock entry, so it
// needs no API version bump and works with any runtime that exports
// it. It runs only while the guest is up: the application starts it
// when the guest reaches its running state and stops it when the
// guest stops. While it is stopped, nothing listens on the host's
// localhost, so a visit to a guest service's address gets a clean
// connection refused, the way it would on a native OS with no
// service running.
//
// Protocol (proxy -> bridge, over one vsock stream per TCP
// connection): 2 bytes, big-endian uint16_t, the target TCP port.
// Then both sides shuttle bytes until either closes. If the guest
// has nothing on that port the bridge closes the vsock stream, and
// the proxy closes the host side cleanly. Port 0 is a control
// request: the bridge replies with the guest's listening ports
// (2-byte count, then 2 bytes per port, all big-endian).
class LocalhostProxy {
public:
    using ConnectVsock = kernarium_runtime_connect_vsock_function;

    // connectVsock is the runtime API's connect_vsock entry. It must
    // stay valid while the proxy runs.
    explicit LocalhostProxy(ConnectVsock connectVsock);
    ~LocalhostProxy();

    LocalhostProxy(const LocalhostProxy&) = delete;
    LocalhostProxy& operator=(const LocalhostProxy&) = delete;

    // The ports forwarded while the guest is up, as the application
    // configured them. Ports are sorted, unique, and in 1...65535;
    // anything else is dropped. Takes effect on the next start, or
    // at once while running.
    void setConfiguredPorts(std::vector<uint16_t> ports);
    std::vector<uint16_t> configuredPorts() const;

    // Start listening on 127.0.0.1 for each configured port, and
    // start the 1 s auto-mirror poll. Starting while running
    // restarts with the current configured ports. Safe to call from
    // any thread.
    void start();
    // Stop all listeners. In-flight proxied connections are closed.
    // Safe to call from any thread.
    void stop();

    bool running() const;
    // The ports currently forwarded (configured plus auto-mirrored).
    std::vector<uint16_t> ports() const;
    // The ports the guest is currently listening on, as last polled
    // from the bridge (the auto-mirror). Empty while the proxy is
    // stopped or before the first poll.
    std::vector<uint16_t> autoPorts() const;

private:
    void acceptLoop();
    void pollLoop();
    // Ask the bridge for the guest's listening ports (a port-0
    // control request). Nullopt when the bridge does not answer
    // (guest still booting, bridge not up yet, or an older bridge
    // without the list request); an empty vector when it answers
    // with no listening ports. Blocking; called on the poll thread.
    std::optional<std::vector<uint16_t>> fetchGuestPorts();
    // Reconcile the listeners with the guest's current listening
    // ports. Called on the poll thread.
    void reconcile(const std::vector<uint16_t>& guestPorts);
    void startListener(uint16_t port);
    void stopListener(uint16_t port);

    ConnectVsock connectVsock_;
    mutable std::mutex mutex_;
    std::condition_variable stopCv_;
    // Guarded by mutex_.
    std::set<uint16_t> configuredPorts_;
    std::set<uint16_t> autoPorts_;
    // One listening fd per forwarded port, guarded by mutex_.
    std::map<uint16_t, int> listenFds_;
    bool stopRequested_ = false;
    std::thread acceptThread_;
    std::thread pollThread_;
    std::atomic<bool> running_{false};
    // Shared with the detached per-connection threads, so they can
    // stop shuttling when the proxy stops without touching this
    // object after it is gone.
    std::shared_ptr<std::atomic<bool>> stopping_ =
        std::make_shared<std::atomic<bool>>(false);
};

}  // namespace kernarium

#endif
