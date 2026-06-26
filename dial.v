module raknet

import net
import rand
import time
import message

pub struct Dialer {
pub mut:
	max_mtu u16
	timeout time.Duration
}

const mtu_probe_sizes = [u16(max_mtu_size), 1200, 576]
const client_loop_read_timeout = 250 * time.millisecond

pub fn ping(address string) ![]u8 {
	return Dialer{}.ping_timeout(address, 5 * time.second)
}

pub fn ping_timeout(address string, timeout time.Duration) ![]u8 {
	return Dialer{}.ping_timeout(address, timeout)
}

pub fn (dialer Dialer) ping(address string) ![]u8 {
	return dialer.ping_timeout(address, 5 * time.second)
}

pub fn (dialer Dialer) ping_timeout(address string, timeout time.Duration) ![]u8 {
	mut udp := net.dial_udp(address)!
	udp.set_read_timeout(timeout)
	defer {
		udp.close() or {}
	}
	udp.write(message.UnconnectedPing{
		ping_time:   timestamp()
		client_guid: rand.i64()
	}.encode())!
	mut buf := []u8{len: int(max_mtu_size)}
	n, _ := udp.read(mut buf)!
	if n == 0 || buf[0] != message.id_unconnected_pong {
		return error('expected unconnected pong')
	}
	pong := message.decode_unconnected_pong(buf[1..n])!
	return pong.data
}

pub fn dial(address string) !&Conn {
	return Dialer{}.dial(address)
}

pub fn dial_timeout(address string, timeout time.Duration) !&Conn {
	return Dialer{
		timeout: timeout
	}.dial(address)
}

pub fn (dialer Dialer) dial(address string) !&Conn {
	mut udp := net.dial_udp(address)!
	mut handoff := false
	defer {
		if !handoff {
			udp.close() or {}
		}
	}
	timeout := if dialer.timeout == 0 { 10 * time.second } else { dialer.timeout }
	udp.set_read_timeout(100 * time.millisecond)
	client_id := -rand.int63()

	mut buf := []u8{len: 1500}
	reply1, remote := dialer.discover_mtu(mut udp, mut buf, timeout)!

	req2 := message.OpenConnectionRequest2{
		server_address:      addr_port_from_string(address)!
		mtu:                 reply1.mtu
		client_guid:         client_id
		server_has_security: reply1.server_has_security
		cookie:              reply1.cookie
	}
	udp.write(req2.encode())!
	n2, _ := udp.read(mut buf)!
	if n2 == 0 || buf[0] != message.id_open_connection_reply_2 {
		return error('expected open connection reply 2')
	}
	reply2 := message.decode_open_connection_reply_2(buf[1..n2])!

	mut conn := new_conn(mut udp, remote, reply2.mtu, false, unsafe { nil })
	udp.set_read_timeout(client_loop_read_timeout)
	spawn client_loop(mut conn)
	conn.write(message.ConnectionRequest{
		client_guid:  client_id
		request_time: timestamp()
	}.encode())!
	wait_connected(conn, timeout) or {
		conn.close_immediately()
		return err
	}
	handoff = true
	return conn
}

fn (dialer Dialer) discover_mtu(mut udp net.UdpConn, mut buf []u8, timeout time.Duration) !(message.OpenConnectionReply1, net.Addr) {
	start := time.now()
	for mtu in dialer.mtu_probe_sizes() {
		for _ in 0 .. 4 {
			udp.write(message.OpenConnectionRequest1{
				client_protocol: protocol_version
				mtu:             mtu
			}.encode())!
			n, remote := udp.read(mut buf) or {
				if time.now() - start >= timeout {
					return error('open connection request 1 timed out')
				}
				continue
			}
			if n == 0 {
				continue
			}
			match buf[0] {
				message.id_open_connection_reply_1 {
					reply := message.decode_open_connection_reply_1(buf[1..n])!
					return reply, remote
				}
				message.id_incompatible_protocol_version {
					response := message.decode_incompatible_protocol_version(buf[1..n])!
					return error('mismatched protocol: client protocol = ${protocol_version}, server protocol = ${response.server_protocol}')
				}
				else {}
			}
		}
	}
	return error('expected open connection reply 1')
}

fn (dialer Dialer) mtu_probe_sizes() []u16 {
	max_mtu := clamp_mtu(dialer.max_mtu, 576)
	if max_mtu == max_mtu_size {
		return mtu_probe_sizes.clone()
	}
	mut sizes := [max_mtu]
	for mtu in mtu_probe_sizes {
		if mtu < max_mtu {
			sizes << mtu
		}
	}
	return sizes
}

fn client_loop(mut conn Conn) {
	for !conn.is_closed() {
		mut buf := []u8{len: 1500}
		n, _ := conn.udp.read(mut buf) or {
			if conn.is_closed() {
				break
			}
			continue
		}
		if n == 0 {
			continue
		}
		conn.receive(buf[..n]) or {}
	}
}
