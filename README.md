# raknet

`raknet` is a RakNet implementation for V, focused on the classic RakNet protocol used by Minecraft: Bedrock Edition.

The API is plain-structed and inspired by Sandertv's `go-raknet`.

## Basic Server

```v
import raknet

mut listener := raknet.listen('0.0.0.0:19132')!
defer {
	listener.close() or {}
}

listener.set_pong_data('raknet server'.bytes())

mut conn := listener.accept()!
mut buf := []u8{len: 4096}
n := conn.read(mut buf)!
conn.write(buf[..n])!
```

## Basic Client

```v
import raknet

mut conn := raknet.dial('127.0.0.1:19132')!
defer {
	conn.close() or {}
}

conn.write('ping'.bytes())!
packet := conn.read_packet()!
println(packet.bytestr())
```

## Basic Ping

```v
import raknet

data := raknet.ping('127.0.0.1:19132')!
println(data.bytestr())
```

IPv6 addresses use bracket notation:

```v
mut conn := raknet.dial('[::1]:19133')!
```

## Configuration

```v
import time
import raknet

mut listener := raknet.ListenConfig{
	max_mtu: 1200
	disable_cookies: false
	handshake_timeout: 10 * time.second
}.listen('0.0.0.0:19132')!

dialer := raknet.Dialer{
	max_mtu: 1200
	timeout: 2 * time.second
}
mut conn := dialer.dial(listener.addr())!
```

## **Notes**

- `listen`, `dial`, `ping`, `read`, `read_packet`, `write` and `close` are the main public API surface.
- `write` sends ReliableOrdered payloads. `write_reliable`, `write_unreliable`, `write_reliable_ordered`, `write_unreliable_sequenced` and `write_reliable_sequenced` are available for lower-level transports.
- `write([]u8{})` returns an error; empty RakNet payloads are not sent.
- Client-side connections own their UDP socket. Server-side connections share the listener socket.
- `Conn` has app-level read/write deadlines, read/write timeouts, idle timeout and keepalive interval tuning.
- `set_pong_data` sets static unconnected pong data. `set_pong_data_func` can generate it per remote address.
- `block` and `block_for` can temporarily ignore packets from an address.
- Public error constants expose stable `err.code()` values for common lifecycle, deadline and protocol failures.

## Tests

```sh
v test .
```
