// SPDX-License-Identifier: GPL-2.0-or-later
// KernariumLocalhostProxy - see the header.
#include "KernariumLocalhostProxy.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace kernarium {

// The guest bridge's well-known vsock port (see
// tools/localhost-bridge/tx-vsock-bridge.c). One listener handles
// every forwarded port: the proxy sends the target TCP port in the
// first two bytes of each vsock stream.
constexpr uint32_t kBridgeVsockPort = 5999;

// How long a vsock connect may take, and how often to retry when the
// bridge is not listening yet (it starts with the guest via systemd,
// so the first connection after boot can race it). Three attempts
// half a second apart is enough for the bridge to come up without
// making a refused connection wait noticeably.
constexpr uint32_t kVsockTimeoutMs = 2000;
constexpr int kVsockRetries = 3;
constexpr int kVsockRetryDelayMs = 500;

// How often the proxy asks the bridge for the guest's listening
// ports (the auto-mirror). One second is fast enough that a newly
// started agent is reachable almost at once, and the poll is a
// single short vsock stream, so it costs nothing measurable.
constexpr std::chrono::seconds kPollInterval{1};

// How long the accept loop and the connection shuttle wait in poll
// before checking whether the proxy is stopping.
constexpr int kAcceptPollTimeoutMs = 100;
constexpr int kShuttlePollTimeoutMs = 500;

namespace {

// Write all bytes, retrying on EINTR and partial writes.
bool writeAll(int fd, const void* buf, size_t len) {
    const auto* p = static_cast<const unsigned char*>(buf);
    while (len > 0) {
        ssize_t w = write(fd, p, len);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        p += static_cast<size_t>(w);
        len -= static_cast<size_t>(w);
    }
    return true;
}

// Read exactly len bytes, retrying on EINTR. False on EOF or error.
bool readExact(int fd, void* buf, size_t len) {
    auto* p = static_cast<unsigned char*>(buf);
    while (len > 0) {
        ssize_t r = read(fd, p, len);
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (r == 0) {
            return false;
        }
        p += static_cast<size_t>(r);
        len -= static_cast<size_t>(r);
    }
    return true;
}

// Close a stream cleanly: discard whatever the peer already sent
// (a client sends its request the moment it connects, and it may
// still be in flight when the vsock side fails), so the close sends
// FIN instead of RST. A close with unread received data sends RST,
// which the host side reports as "connection reset".
void closeCleanly(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    char buf[4096];
    for (int round = 0; round < 3; ++round) {
        for (;;) {
            ssize_t r = read(fd, buf, sizeof(buf));
            if (r <= 0) {
                break;
            }
        }
        struct timespec ts = {0, 5 * 1000 * 1000};
        nanosleep(&ts, nullptr);
    }
    close(fd);
}

// Open a vsock stream to the bridge, retrying a refused connection
// a few times: the bridge starts with the guest, so the first
// connection after boot can race it. Returns the fd, or a negative
// errno.
int connectBridge(LocalhostProxy::ConnectVsock connectVsock) {
    if (!connectVsock) {
        return -ENODEV;
    }
    int fd = -ENODEV;
    for (int attempt = 0; attempt < kVsockRetries; ++attempt) {
        fd = connectVsock(kBridgeVsockPort, kVsockTimeoutMs);
        if (fd >= 0) {
            break;
        }
        if (fd == -ECONNREFUSED && attempt + 1 < kVsockRetries) {
            struct timespec ts = {0, kVsockRetryDelayMs * 1000 * 1000};
            nanosleep(&ts, nullptr);
            continue;
        }
        break;
    }
    return fd;
}

// Shuttle bytes between two fds until both sides are done or the
// proxy stops. A half close on one side shuts the write side of the
// other, so a service that closes after its reply still lets the
// reply through. Mirrors the bridge's shuttle
// (tools/localhost-bridge/tx-vsock-bridge.c).
void shuttle(int a, int b,
              const std::shared_ptr<std::atomic<bool>>& stopping) {
    char buf[16384];
    struct pollfd pfd[2];
    bool aOpen = true;
    bool bOpen = true;
    pfd[0].fd = a;
    pfd[1].fd = b;
    while ((aOpen || bOpen) && !stopping->load()) {
        pfd[0].events = aOpen ? POLLIN : 0;
        pfd[1].events = bOpen ? POLLIN : 0;
        if (!aOpen && !bOpen) {
            break;
        }
        int n = poll(pfd, 2, kShuttlePollTimeoutMs);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (n == 0) {
            continue;
        }
        if ((pfd[0].revents & (POLLIN | POLLHUP | POLLERR)) && aOpen) {
            ssize_t r = read(a, buf, sizeof(buf));
            if (r > 0) {
                ssize_t off = 0;
                while (off < r) {
                    ssize_t w = write(b, buf + off, static_cast<size_t>(r - off));
                    if (w < 0) {
                        if (errno == EINTR) {
                            continue;
                        }
                        break;
                    }
                    off += w;
                }
                if (off < r) {
                    shutdown(a, SHUT_RD);
                    aOpen = false;
                }
            } else if (r == 0) {
                shutdown(b, SHUT_WR);
                aOpen = false;
            } else if (errno != EINTR) {
                shutdown(b, SHUT_WR);
                aOpen = false;
            }
        }
        if ((pfd[1].revents & (POLLIN | POLLHUP | POLLERR)) && bOpen) {
            ssize_t r = read(b, buf, sizeof(buf));
            if (r > 0) {
                ssize_t off = 0;
                while (off < r) {
                    ssize_t w = write(a, buf + off, static_cast<size_t>(r - off));
                    if (w < 0) {
                        if (errno == EINTR) {
                            continue;
                        }
                        break;
                    }
                    off += w;
                }
                if (off < r) {
                    shutdown(b, SHUT_RD);
                    bOpen = false;
                }
            } else if (r == 0) {
                shutdown(a, SHUT_WR);
                bOpen = false;
            } else if (errno != EINTR) {
                shutdown(a, SHUT_WR);
                bOpen = false;
            }
        }
    }
}

// One proxied connection: open a vsock stream to the bridge, send
// the target port, shuttle both ways. Runs on a detached thread and
// touches no LocalhostProxy state, only the copied connect function
// and stopping flag, so it is safe after the proxy is gone.
void handleConnection(int tcpFd, uint16_t port,
                      LocalhostProxy::ConnectVsock connectVsock,
                      std::shared_ptr<std::atomic<bool>> stopping) {
    int vsockFd = connectBridge(connectVsock);
    if (vsockFd < 0) {
        // No guest, or no bridge: the host side closes cleanly, so
        // the client sees an empty reply, not a reset.
        std::fprintf(stderr,
                     "Localhost proxy: vsock to bridge failed for port %u: %s\n",
                     port, std::strerror(-vsockFd));
        closeCleanly(tcpFd);
        return;
    }
    // The target port, big-endian, then shuttle both ways.
    unsigned char portBytes[2] = {
        static_cast<unsigned char>((port >> 8) & 0xff),
        static_cast<unsigned char>(port & 0xff),
    };
    if (!writeAll(vsockFd, portBytes, sizeof(portBytes))) {
        close(vsockFd);
        closeCleanly(tcpFd);
        return;
    }
    shuttle(tcpFd, vsockFd, stopping);
    close(vsockFd);
    close(tcpFd);
}

std::vector<uint16_t> sortedUnique(std::vector<uint16_t> ports) {
    std::sort(ports.begin(), ports.end());
    ports.erase(std::unique(ports.begin(), ports.end()), ports.end());
    return ports;
}

}  // namespace

LocalhostProxy::LocalhostProxy(ConnectVsock connectVsock)
    : connectVsock_(connectVsock) {}

LocalhostProxy::~LocalhostProxy() {
    stop();
}

void LocalhostProxy::setConfiguredPorts(std::vector<uint16_t> ports) {
    std::set<uint16_t> clean;
    for (uint16_t port : ports) {
        if (port >= 1) {
            clean.insert(port);
        }
    }
    bool wasRunning = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        configuredPorts_ = clean;
        wasRunning = running_.load();
    }
    if (wasRunning) {
        std::lock_guard<std::mutex> lock(mutex_);
        reconcile(std::vector<uint16_t>(autoPorts_.begin(), autoPorts_.end()));
    }
}

std::vector<uint16_t> LocalhostProxy::configuredPorts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::vector<uint16_t>(configuredPorts_.begin(), configuredPorts_.end());
}

void LocalhostProxy::start() {
    if (!connectVsock_) {
        std::fprintf(stderr, "Localhost proxy: no runtime vsock API, not starting\n");
        return;
    }
    stop();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // A fresh stopping flag: any detached connection thread
        // from an earlier run keeps the old one (already set) and
        // exits, while this run's threads use the new one.
        stopping_ = std::make_shared<std::atomic<bool>>(false);
        stopRequested_ = false;
        autoPorts_.clear();
        for (uint16_t port : configuredPorts_) {
            startListener(port);
        }
        running_.store(true);
    }
    acceptThread_ = std::thread([this] { acceptLoop(); });
    pollThread_ = std::thread([this] { pollLoop(); });
    std::fprintf(stderr, "Localhost proxy: listening on 127.0.0.1\n");
}

void LocalhostProxy::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_.load() && !acceptThread_.joinable() && !pollThread_.joinable()) {
            return;
        }
        stopping_->store(true);
        stopRequested_ = true;
    }
    stopCv_.notify_all();
    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
    if (pollThread_.joinable()) {
        pollThread_.join();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [port, fd] : listenFds_) {
            (void)port;
            close(fd);
        }
        listenFds_.clear();
        autoPorts_.clear();
        running_.store(false);
    }
    std::fprintf(stderr, "Localhost proxy: stopped\n");
}

bool LocalhostProxy::running() const {
    return running_.load();
}

std::vector<uint16_t> LocalhostProxy::ports() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint16_t> ports;
    ports.reserve(listenFds_.size());
    for (const auto& [port, fd] : listenFds_) {
        (void)fd;
        ports.push_back(port);
    }
    return ports;
}

std::vector<uint16_t> LocalhostProxy::autoPorts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::vector<uint16_t>(autoPorts_.begin(), autoPorts_.end());
}

void LocalhostProxy::acceptLoop() {
    for (;;) {
        if (stopping_->load()) {
            return;
        }
        std::vector<std::pair<uint16_t, int>> listeners;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            listeners.reserve(listenFds_.size());
            for (const auto& [port, fd] : listenFds_) {
                listeners.emplace_back(port, fd);
            }
        }
        if (listeners.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kAcceptPollTimeoutMs));
            continue;
        }
        std::vector<struct pollfd> pfds(listeners.size());
        for (size_t i = 0; i < listeners.size(); ++i) {
            pfds[i].fd = listeners[i].second;
            pfds[i].events = POLLIN;
            pfds[i].revents = 0;
        }
        int n = poll(pfds.data(), static_cast<nfds_t>(pfds.size()), kAcceptPollTimeoutMs);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return;
        }
        if (n == 0) {
            continue;
        }
        for (size_t i = 0; i < listeners.size(); ++i) {
            if (!(pfds[i].revents & (POLLIN | POLLHUP | POLLERR))) {
                continue;
            }
            uint16_t port = listeners[i].first;
            int listenFd = listeners[i].second;
            for (;;) {
                int conn = accept(listenFd, nullptr, nullptr);
                if (conn < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    if (errno == EINTR) {
                        continue;
                    }
                    std::fprintf(stderr,
                                 "Localhost proxy: accept on %u failed: %s\n",
                                 port, std::strerror(errno));
                    break;
                }
                // Hand the connection to its own thread: the vsock
                // connect can block, and the shuttle runs until the
                // connection closes.
                ConnectVsock connectVsock = connectVsock_;
                std::shared_ptr<std::atomic<bool>> stopping = stopping_;
                std::thread([conn, port, connectVsock, stopping] {
                    handleConnection(conn, port, connectVsock, stopping);
                }).detach();
            }
        }
    }
}

void LocalhostProxy::pollLoop() {
    for (;;) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (stopCv_.wait_for(lock, kPollInterval,
                                 [this] { return stopRequested_; })) {
                return;
            }
        }
        if (stopping_->load()) {
            return;
        }
        std::optional<std::vector<uint16_t>> guestPorts = fetchGuestPorts();
        if (!guestPorts) {
            // The bridge did not answer (guest still booting, or an
            // older bridge without the list request): keep the last
            // known auto ports and retry next tick.
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            reconcile(*guestPorts);
        }
    }
}

std::optional<std::vector<uint16_t>> LocalhostProxy::fetchGuestPorts() {
    int vsockFd = connectBridge(connectVsock_);
    if (vsockFd < 0) {
        return std::nullopt;
    }
    // Port 0, big-endian, then read the reply: 2-byte count, then 2
    // bytes per port, all big-endian.
    unsigned char req[2] = {0, 0};
    if (!writeAll(vsockFd, req, sizeof(req))) {
        close(vsockFd);
        return std::nullopt;
    }
    unsigned char hdr[2];
    if (!readExact(vsockFd, hdr, sizeof(hdr))) {
        close(vsockFd);
        return std::nullopt;
    }
    uint16_t count = static_cast<uint16_t>((hdr[0] << 8) | hdr[1]);
    std::vector<uint16_t> ports;
    ports.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        unsigned char pb[2];
        if (!readExact(vsockFd, pb, sizeof(pb))) {
            close(vsockFd);
            return std::nullopt;
        }
        uint16_t port = static_cast<uint16_t>((pb[0] << 8) | pb[1]);
        if (port >= 1) {
            ports.push_back(port);
        }
    }
    close(vsockFd);
    return sortedUnique(std::move(ports));
}

void LocalhostProxy::reconcile(const std::vector<uint16_t>& guestPorts) {
    // Called with mutex_ held.
    std::set<uint16_t> guestSet(guestPorts.begin(), guestPorts.end());
    if (guestSet == autoPorts_) {
        return;
    }
    autoPorts_ = guestSet;
    std::set<uint16_t> wanted = configuredPorts_;
    wanted.insert(guestSet.begin(), guestSet.end());
    for (uint16_t port : wanted) {
        if (listenFds_.find(port) == listenFds_.end()) {
            startListener(port);
        }
    }
    // Collect the ports to drop first: stopListener erases from the
    // map, so erasing while iterating it would invalidate the
    // iterator.
    std::vector<uint16_t> toStop;
    for (const auto& [port, fd] : listenFds_) {
        (void)fd;
        if (wanted.find(port) == wanted.end()) {
            toStop.push_back(port);
        }
    }
    for (uint16_t port : toStop) {
        stopListener(port);
    }
}

void LocalhostProxy::startListener(uint16_t port) {
    // Called with mutex_ held.
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::fprintf(stderr, "Localhost proxy: socket for port %u failed: %s\n",
                     port, std::strerror(errno));
        return;
    }
    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::fprintf(stderr, "Localhost proxy: bind 127.0.0.1:%u failed: %s\n",
                     port, std::strerror(errno));
        close(fd);
        return;
    }
    if (listen(fd, 64) != 0) {
        std::fprintf(stderr, "Localhost proxy: listen 127.0.0.1:%u failed: %s\n",
                     port, std::strerror(errno));
        close(fd);
        return;
    }
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    listenFds_[port] = fd;
}

void LocalhostProxy::stopListener(uint16_t port) {
    // Called with mutex_ held. The fd is closed here; the accept
    // loop rebuilds its poll set from the map each iteration, so it
    // drops the closed fd within one poll timeout.
    auto it = listenFds_.find(port);
    if (it == listenFds_.end()) {
        return;
    }
    close(it->second);
    listenFds_.erase(it);
}

}  // namespace kernarium
