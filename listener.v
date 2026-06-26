module raknet

import net
import rand
import sync
import time
import message

pub struct ListenConfig {
pub mut:
	disable_cookies   bool
	max_mtu           u16
	block_duration    time.Duration
	capture_packets   bool
	handshake_timeout time.Duration
}

@[heap]
pub struct Listener {
mut:
	udp                  &net.UdpConn = unsafe { nil }
	incoming             chan &Conn
	connections          map[string]&Conn
	connections_mutex    &sync.Mutex = sync.new_mutex()
	id                   i64
	closed               bool
	lifecycle_mutex      &sync.Mutex = sync.new_mutex()
	pong_data            []u8
	pong_data_mutex      &sync.Mutex = sync.new_mutex()
	max_mtu              u16
	disable_cookies      bool
	cookie_salt          u64
	previous_cookie_salt u64
	block_duration       time.Duration
	blocks               map[string]time.Time
	security_mutex       &sync.Mutex = sync.new_mutex()
	capture_packets      bool
	captured             [][]u8
	capture_mutex        &sync.Mutex = sync.new_mutex()
	handshake_timeout    time.Duration
}

pub fn listen(address string) !&Listener {
	return ListenConfig{}.listen(address)
}

pub fn (conf ListenConfig) listen(address string) !&Listener {
	mut udp := net.listen_udp(address)!
	udp.set_read_timeout(5 * time.second)
	max_mtu := clamp_mtu(conf.max_mtu, min_mtu_size)
	block_duration := if conf.block_duration == 0 { 10 * time.second } else { conf.block_duration }
	handshake_timeout := if conf.handshake_timeout == 0 {
		10 * time.second
	} else {
		conf.handshake_timeout
	}
	cookie_salt := rand.u64()
	mut listener := &Listener{
		udp:                  udp
		incoming:             chan &Conn{cap: 32}
		connections:          map[string]&Conn{}
		connections_mutex:    sync.new_mutex()
		id:                   rand.i64()
		lifecycle_mutex:      sync.new_mutex()
		pong_data:            []u8{}
		pong_data_mutex:      sync.new_mutex()
		max_mtu:              max_mtu
		disable_cookies:      conf.disable_cookies
		cookie_salt:          cookie_salt
		previous_cookie_salt: cookie_salt
		block_duration:       block_duration
		blocks:               map[string]time.Time{}
		security_mutex:       sync.new_mutex()
		capture_packets:      conf.capture_packets
		captured:             [][]u8{}
		capture_mutex:        sync.new_mutex()
		handshake_timeout:    handshake_timeout
	}
	spawn listener.loop()
	spawn listener.security_loop()
	return listener
}

pub fn (l &Listener) addr() string {
	return l.udp.sock.address() or { return '' }.str()
}

pub fn (l &Listener) accept() !&Conn {
	conn := <-l.incoming or { return error('listener closed') }
	return conn
}

pub fn (l &Listener) accept_timeout(timeout time.Duration) !&Conn {
	mut conn := unsafe { nil }
	mut timed_out := false
	open := select {
		conn = <-l.incoming {}
		timeout {
			timed_out = true
		}
	}
	if timed_out {
		return error('accept timed out')
	}
	if !open || conn == unsafe { nil } {
		return error('listener closed')
	}
	return conn
}

pub fn (mut l Listener) close() ! {
	mut conns := []&Conn{}
	l.lifecycle_mutex.lock()
	if l.closed {
		l.lifecycle_mutex.unlock()
		return
	}
	l.closed = true
	if !l.incoming.closed {
		l.incoming.close()
	}
	l.lifecycle_mutex.unlock()
	l.connections_mutex.lock()
	for _, conn in l.connections {
		conns << conn
	}
	l.connections.clear()
	l.connections_mutex.unlock()
	for conn in conns {
		conn.close_immediately()
	}
	l.udp.close() or {}
}

pub fn (mut l Listener) set_pong_data(data []u8) {
	if data.len > max_i16 {
		panic('pong data must be no longer than ${max_i16} bytes')
	}
	l.pong_data_mutex.lock()
	l.pong_data = data.clone()
	l.pong_data_mutex.unlock()
}

pub fn (mut l Listener) captured_packets() [][]u8 {
	l.capture_mutex.lock()
	mut packets := [][]u8{cap: l.captured.len}
	for packet in l.captured {
		packets << packet.clone()
	}
	l.capture_mutex.unlock()
	return packets
}

fn (mut l Listener) loop() {
	for !l.is_closed() {
		mut buf := []u8{len: 1500}
		n, addr := l.udp.read(mut buf) or { continue }
		if n == 0 {
			continue
		}
		l.capture_packet(buf[..n])
		l.handle(buf[..n], addr) or { l.block_addr(addr) }
	}
}

fn (mut l Listener) capture_packet(data []u8) {
	if !l.capture_packets {
		return
	}
	l.capture_mutex.lock()
	l.captured << data.clone()
	l.capture_mutex.unlock()
}

fn (mut l Listener) handle(data []u8, addr net.Addr) ! {
	if l.addr_blocked(addr) {
		return
	}
	key := normalise_addr_string(addr.str())
	if l.receive_for_key(key, data)! {
		return
	}
	match data[0] {
		message.id_unconnected_ping, message.id_unconnected_ping_open_connections {
			ping_packet := message.decode_unconnected_ping(data[1..])!
			pong_data := l.pong_data_clone()
			l.udp.write_to(addr, message.UnconnectedPong{
				ping_time:   ping_packet.ping_time
				server_guid: l.id
				data:        pong_data
			}.encode())!
		}
		message.id_open_connection_request_1 {
			req := message.decode_open_connection_request_1(data[1..])!
			if req.client_protocol != protocol_version {
				l.udp.write_to(addr, message.IncompatibleProtocolVersion{
					server_protocol: protocol_version
					server_guid:     l.id
				}.encode())!
				return error('incompatible protocol')
			}
			mtu := if req.mtu < l.max_mtu { req.mtu } else { l.max_mtu }
			l.udp.write_to(addr, message.OpenConnectionReply1{
				server_guid:         l.id
				server_has_security: !l.disable_cookies
				cookie:              l.cookie(addr)
				mtu:                 mtu
			}.encode())!
		}
		message.id_open_connection_request_2 {
			req := message.decode_open_connection_request_2(data[1..], !l.disable_cookies)!
			if !l.disable_cookies && req.cookie != l.cookie(addr)
				&& req.cookie != cookie_for_addr(addr, l.previous_cookie_salt) {
				l.block_addr(addr)
				return error('invalid cookie')
			}
			if req.client_guid >= 0 {
				l.block_addr(addr)
				return error('invalid client guid')
			}
			mtu := if req.mtu < l.max_mtu { req.mtu } else { l.max_mtu }
			client_addr := addr_port_from_string(addr.str()) or { message.AddrPort{} }
			l.udp.write_to(addr, message.OpenConnectionReply2{
				server_guid:    l.id
				client_address: client_addr
				mtu:            mtu
			}.encode())!
			mut conn := new_conn(mut l.udp, addr, mtu, true, l)
			l.put_conn(key, conn)
			spawn l.cleanup_pending_handshake(key, conn)
		}
		else {
			return
		}
	}
}

fn (mut l Listener) is_closed() bool {
	l.lifecycle_mutex.lock()
	closed := l.closed
	l.lifecycle_mutex.unlock()
	return closed
}

fn (mut l Listener) receive_for_key(key string, data []u8) !bool {
	l.connections_mutex.lock()
	mut active_conn := l.connections[key] or {
		l.connections_mutex.unlock()
		return false
	}
	l.connections_mutex.unlock()
	active_conn.receive(data)!
	return true
}

fn (mut l Listener) put_conn(key string, conn &Conn) {
	l.connections_mutex.lock()
	l.connections[key] = conn
	l.connections_mutex.unlock()
}

fn (mut l Listener) delete_conn(key string) {
	l.connections_mutex.lock()
	l.connections.delete(key)
	l.connections_mutex.unlock()
}

fn (mut l Listener) delete_conn_if_same(key string, conn &Conn) {
	l.connections_mutex.lock()
	active_conn := l.connections[key] or {
		l.connections_mutex.unlock()
		return
	}
	if active_conn == conn {
		l.connections.delete(key)
	}
	l.connections_mutex.unlock()
}

fn (mut l Listener) cleanup_pending_handshake(key string, conn &Conn) {
	time.sleep(l.handshake_timeout)
	if conn.is_connected_confirmed() || conn.is_closed() {
		return
	}
	l.delete_conn_if_same(key, conn)
	conn.close_immediately()
}

fn (mut l Listener) queue_incoming(conn &Conn) {
	if l.is_closed() {
		return
	}
	l.incoming <- conn or {}
}

fn (mut l Listener) cookie(addr net.Addr) u32 {
	if l.disable_cookies {
		return 0
	}
	l.security_mutex.lock()
	salt := l.cookie_salt
	l.security_mutex.unlock()
	return cookie_for_addr(addr, salt)
}

fn (mut l Listener) rotate_cookie_salt() {
	l.security_mutex.lock()
	l.previous_cookie_salt = l.cookie_salt
	l.cookie_salt = rand.u64()
	l.security_mutex.unlock()
}

fn (mut l Listener) security_loop() {
	mut ticks := 0
	for !l.is_closed() {
		time.sleep(time.second)
		l.gc_blocks()
		ticks++
		if ticks % 2 == 0 {
			l.rotate_cookie_salt()
		}
	}
}

fn (mut l Listener) block_addr(addr net.Addr) {
	if l.block_duration <= 0 {
		return
	}
	l.security_mutex.lock()
	l.blocks[block_key(addr)] = time.now().add(l.block_duration)
	l.security_mutex.unlock()
}

fn (mut l Listener) addr_blocked(addr net.Addr) bool {
	key := block_key(addr)
	l.security_mutex.lock()
	expires := l.blocks[key] or {
		l.security_mutex.unlock()
		return false
	}
	if time.now() > expires {
		l.blocks.delete(key)
		l.security_mutex.unlock()
		return false
	}
	l.security_mutex.unlock()
	return true
}

fn (mut l Listener) gc_blocks() {
	l.security_mutex.lock()
	now := time.now()
	for key, expires in l.blocks {
		if now > expires {
			l.blocks.delete(key)
		}
	}
	l.security_mutex.unlock()
}

fn (mut l Listener) pong_data_clone() []u8 {
	l.pong_data_mutex.lock()
	data := l.pong_data.clone()
	l.pong_data_mutex.unlock()
	return data
}

fn block_key(addr net.Addr) string {
	s := addr.str()
	if s.starts_with('[') {
		end := s.index(']') or { return s }
		return s[1..end]
	}
	parts := s.split(':')
	if parts.len <= 1 {
		return s
	}
	return parts[..parts.len - 1].join(':')
}
