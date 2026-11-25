# Localhost bridge

The guest half of the localhost proxy. A service listening on
127.0.0.1 inside the guest is not reachable from the host (the guest
sits behind slirp NAT, outbound only), so this bridge listens on
vsock port 5999 and forwards each connection to the guest's
127.0.0.1. The host half is `KernariumLocalhostProxy` in
`Kernarium/Runtime/`: it listens on the host's 127.0.0.1 and proxies
each connection here over vsock, so a guest localhost service answers
at the host's localhost.

## Protocol

One vsock stream per TCP connection. The host sends 2 bytes,
big-endian, the target TCP port, then both sides shuttle bytes until
either closes. If nothing listens on that port the bridge closes the
stream, so the host side sees EOF.

Port 0 is a control request: the bridge replies with the guest's
listening TCP ports (2-byte count, then 2 bytes per port, all
big-endian), sorted, and closes. The list comes from a netlink
sock_diag scan of TCP LISTEN sockets, falling back to
`/proc/net/tcp`, filtered to services reachable on guest localhost
(127.0.0.1, 0.0.0.0, or the slirp guest IP 10.0.2.15). The host
proxy polls this once a second and forwards those ports too, so any
service that starts listening in the guest is reachable with no
per-port configuration.

## Build and install

Static, no dependencies beyond libc:

```sh
aarch64-linux-gnu-gcc -static -O2 -o tx-vsock-bridge tx-vsock-bridge.c
```

Install the binary at `/usr/local/bin/tx-vsock-bridge` (mode 0755)
and the unit at `/etc/systemd/system/tx-vsock-bridge.service`
(mode 0644), then `systemctl enable tx-vsock-bridge.service`. It
starts at boot and restarts always. It logs only startup and errors,
to stderr.
