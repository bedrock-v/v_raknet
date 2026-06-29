module raknet

import net
import sync
import time
import message

fn accept_and_echo(listener &Listener, done chan bool) {
	mut conn := listener.accept() or {
		done <- false
		return
	}
	mut buf := []u8{len: 16}
	n := conn.read(mut buf) or {
		done <- false
		return
	}
	if buf[..n].bytestr() != 'ping' {
		done <- false
		return
	}
	conn.write('pong'.bytes()) or {
		done <- false
		return
	}
	done <- true
}

fn accept_large_payload(listener &Listener, done chan bool) {
	mut conn := listener.accept() or {
		done <- false
		return
	}
	mut buf := []u8{len: 4096}
	n := conn.read(mut buf) or {
		done <- false
		return
	}
	if n != 3000 {
		done <- false
		return
	}
	for i in 0 .. n {
		if buf[i] != u8(i % 251) {
			done <- false
			return
		}
	}
	done <- true
}

fn wait_listener_block_count(mut listener Listener, min_count int, timeout time.Duration) bool {
	deadline := time.now().add(timeout)
	for time.now() < deadline {
		listener.security_mutex.lock()
		count := listener.blocks.len
		listener.security_mutex.unlock()
		if count >= min_count {
			return true
		}
		time.sleep(10 * time.millisecond)
	}
	return false
}

fn test_local_listen_dial_write_read() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	done := chan bool{cap: 1}
	spawn accept_and_echo(listener, done)

	mut conn := dial(listener.addr()) or { panic(err) }
	defer {
		conn.close() or {}
	}
	conn.write('ping'.bytes()) or { panic(err) }
	mut buf := []u8{len: 16}
	n := conn.read(mut buf) or { panic(err) }
	assert buf[..n].bytestr() == 'pong'

	time.sleep(50 * time.millisecond)
	assert <-done
}

fn test_ipv6_addr_port_from_string() {
	addr := addr_port_from_string('[::1]:19133')!
	assert addr.is6
	assert addr.port == 19133
	assert addr.ip6 == [u8(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]!
}

fn test_local_ipv6_listen_dial_write_read() {
	mut listener := listen('[::1]:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	done := chan bool{cap: 1}
	spawn accept_and_echo(listener, done)

	mut conn := dial(listener.addr()) or { panic(err) }
	defer {
		conn.close() or {}
	}
	conn.write('ping'.bytes()) or { panic(err) }
	mut buf := []u8{len: 16}
	n := conn.read(mut buf) or { panic(err) }
	assert buf[..n].bytestr() == 'pong'

	select {
		ok := <-done {
			assert ok
		}
		2 * time.second {
			assert false, 'ipv6 server did not receive echo request'
		}
	}
}

fn test_split_payload_over_small_mtu() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	listener.max_mtu = 400
	defer {
		listener.close() or {}
	}
	done := chan bool{cap: 1}
	spawn accept_large_payload(listener, done)

	mut conn := dial(listener.addr()) or { panic(err) }
	defer {
		conn.close() or {}
	}
	payload := []u8{len: 3000, init: u8(index % 251)}
	conn.write(payload) or { panic(err) }

	select {
		ok := <-done {
			assert ok
		}
		2 * time.second {
			assert false, 'server did not receive reassembled split payload'
		}
	}
}

fn test_conn_delivers_reliable_ordered_packets_in_order() {
	mut conn := &Conn{
		packets:      chan []u8{cap: 4}
		packet_queue: new_packet_queue()
	}
	conn.receive_packet(Packet{
		reliability: .reliable_ordered
		order_index: Uint24(1)
		content:     [u8(0xbb)]
	})!
	select {
		_ := <-conn.packets {
			assert false, 'out-of-order packet was delivered too early'
		}
		else {}
	}
	conn.receive_packet(Packet{
		reliability: .reliable_ordered
		order_index: Uint24(0)
		content:     [u8(0xaa)]
	})!
	assert (<-conn.packets) == [u8(0xaa)]
	assert (<-conn.packets) == [u8(0xbb)]
}

fn test_conn_drops_duplicate_datagrams() {
	mut conn := &Conn{
		mtu:          max_mtu_size
		packets:      chan []u8{cap: 4}
		win:          new_datagram_window()
		packet_queue: new_packet_queue()
		resend:       new_resend_map()
	}
	pk := Packet{
		reliability: .reliable_ordered
		order_index: Uint24(0)
		content:     [u8(0xaa)]
	}
	mut datagram := []u8{}
	datagram << (bit_flag_datagram | bit_flag_needs_b_and_as)
	write_uint24(mut datagram, Uint24(0))
	pk.write(mut datagram)

	conn.receive(datagram)!
	assert (<-conn.packets) == [u8(0xaa)]
	conn.receive(datagram)!
	select {
		_ := <-conn.packets {
			assert false, 'duplicate datagram delivered packet twice'
		}
		else {}
	}
}

fn test_conn_write_fails_after_close() {
	mut conn := &Conn{
		mtu: max_mtu_size
	}
	conn.close()!
	mut failed := false
	conn.write([u8(1)]) or {
		failed = true
		assert err.msg().contains('closed')
	}
	assert failed
}

fn test_listener_replies_incompatible_protocol() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	mut udp := net.dial_udp(listener.addr()) or { panic(err) }
	udp.set_read_timeout(500 * time.millisecond)
	defer {
		udp.close() or {}
	}
	udp.write(message.OpenConnectionRequest1{
		client_protocol: protocol_version - 1
		mtu:             max_mtu_size
	}.encode()) or { panic(err) }

	mut buf := []u8{len: 1500}
	n, _ := udp.read(mut buf) or {
		assert false, 'incompatible protocol read failed: ${err.msg()}'
		return
	}
	assert n > 0
	assert buf[0] == message.id_incompatible_protocol_version
	response := message.decode_incompatible_protocol_version(buf[1..n]) or { panic(err) }
	assert response.server_protocol == protocol_version
}

fn test_listener_rejects_invalid_cookie_when_security_enabled() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	mut udp := net.dial_udp(listener.addr()) or { panic(err) }
	udp.set_read_timeout(500 * time.millisecond)
	defer {
		udp.close() or {}
	}
	request_1 := message.OpenConnectionRequest1{
		client_protocol: protocol_version
		mtu:             max_mtu_size
	}.encode()
	mut buf := []u8{len: 1500}
	mut reply1 := message.OpenConnectionReply1{}
	mut got_reply1 := false
	deadline1 := time.now().add(2 * time.second)
	for time.now() < deadline1 {
		udp.write(request_1) or { panic(err) }
		n1, _ := udp.read(mut buf) or { continue }
		if n1 == 0 || buf[0] != message.id_open_connection_reply_1 {
			continue
		}
		reply1 = message.decode_open_connection_reply_1(buf[1..n1]) or { panic(err) }
		got_reply1 = true
		break
	}
	assert got_reply1
	assert reply1.server_has_security
	assert reply1.cookie != 0

	invalid_request_2 := message.OpenConnectionRequest2{
		server_address:      addr_port_from_string(listener.addr()) or { panic(err) }
		mtu:                 reply1.mtu
		client_guid:         -123
		server_has_security: true
		cookie:              0
	}.encode()
	deadline := time.now().add(2 * time.second)
	for time.now() < deadline {
		udp.write(invalid_request_2) or { panic(err) }
		if wait_listener_block_count(mut listener, 1, 50 * time.millisecond) {
			return
		}
	}
	assert false, 'invalid cookie did not create block'
}

fn test_ping_returns_pong_data() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	listener.set_pong_data('MCPE;V RakNet'.bytes())
	data := ping(listener.addr()) or { panic(err) }
	assert data.bytestr() == 'MCPE;V RakNet'
}

fn test_dialer_uses_configured_max_mtu() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	done := chan bool{cap: 1}
	spawn fn (listener &Listener, done chan bool) {
		mut conn := listener.accept() or {
			done <- false
			return
		}
		done <- (conn.mtu == 576)
	}(listener, done)

	dialer := Dialer{
		max_mtu: 576
	}
	mut conn := dialer.dial(listener.addr()) or { panic(err) }
	defer {
		conn.close() or {}
	}
	assert conn.mtu == 576
	select {
		ok := <-done {
			assert ok
		}
		2 * time.second {
			assert false, 'server did not accept configured MTU dial'
		}
	}
}

fn test_dial_timeout_public_wrapper_connects() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	done := chan bool{cap: 1}
	spawn fn (listener &Listener, done chan bool) {
		mut conn := listener.accept() or {
			done <- false
			return
		}
		done <- true
		conn.close() or {}
	}(listener, done)

	mut conn := dial_timeout(listener.addr(), 2 * time.second) or { panic(err) }
	conn.close() or {}
	select {
		ok := <-done {
			assert ok
		}
		2 * time.second {
			assert false, 'dial_timeout did not connect'
		}
	}
}

fn mtu_probe_fake_server(mut udp net.UdpConn, probes chan u16, done chan bool) {
	mut conn := &Conn{
		udp:          unsafe { &udp }
		mtu:          1200
		packets:      chan []u8{cap: 4}
		connected:    chan bool{cap: 1}
		splits:       map[u16][][]u8{}
		resend:       new_resend_map()
		win:          new_datagram_window()
		packet_queue: new_packet_queue()
		is_server:    true
	}
	mut buf := []u8{len: 1600}
	mut client_addr := net.Addr{}
	for {
		n, addr := udp.read(mut buf) or {
			done <- false
			return
		}
		if n == 0 {
			continue
		}
		if buf[0] == message.id_open_connection_request_1 {
			req := message.decode_open_connection_request_1(buf[1..n]) or {
				done <- false
				return
			}
			probes <- req.mtu
			if req.mtu > 1200 {
				continue
			}
			client_addr = addr
			conn.remote = addr
			conn.remote_key = normalise_addr_string(addr.str())
			udp.write_to(addr, message.OpenConnectionReply1{
				server_guid:         99
				server_has_security: false
				mtu:                 1200
			}.encode()) or {
				done <- false
				return
			}
			break
		}
	}
	for {
		n, addr := udp.read(mut buf) or {
			done <- false
			return
		}
		if n == 0 {
			continue
		}
		if buf[0] == message.id_open_connection_request_2 {
			udp.write_to(addr, message.OpenConnectionReply2{
				server_guid:    99
				client_address: addr_port_from_string(addr.str()) or { message.AddrPort{} }
				mtu:            1200
			}.encode()) or {
				done <- false
				return
			}
			break
		}
	}
	for {
		n, _ := udp.read(mut buf) or {
			done <- false
			return
		}
		if n == 0 {
			continue
		}
		conn.receive(buf[..n]) or {
			done <- false
			return
		}
		select {
			_ := <-conn.connected {
				conn.write('probe-ok'.bytes()) or {
					done <- false
					return
				}
				done <- true
				return
			}
			else {}
		}
	}
	_ = client_addr
}

fn test_dialer_probes_next_mtu_when_largest_probe_gets_no_reply() {
	mut udp := net.listen_udp('127.0.0.1:0') or { panic(err) }
	udp.set_read_timeout(2 * time.second)
	defer {
		udp.close() or {}
	}
	probes := chan u16{cap: 8}
	done := chan bool{cap: 1}
	spawn mtu_probe_fake_server(mut udp, probes, done)

	mut conn := dial(udp.sock.address() or { panic(err) }.str()) or { panic(err) }
	defer {
		conn.close() or {}
	}
	for _ in 0 .. 4 {
		assert <-probes == max_mtu_size
	}
	assert <-probes == 1200
	assert conn.mtu == 1200
	assert conn.read_packet() or { panic(err) }.bytestr() == 'probe-ok'
	assert <-done
}

fn make_reliable_datagram(seq Uint24, order Uint24, content []u8) []u8 {
	pk := Packet{
		reliability: .reliable_ordered
		order_index: order
		content:     content
	}
	mut datagram := []u8{}
	datagram << (bit_flag_datagram | bit_flag_needs_b_and_as)
	write_uint24(mut datagram, seq)
	pk.write(mut datagram)
	return datagram
}

fn test_conn_batches_acknowledgements_until_flush() {
	mut conn := &Conn{
		mtu:          max_mtu_size
		packets:      chan []u8{cap: 4}
		win:          new_datagram_window()
		packet_queue: new_packet_queue()
		resend:       new_resend_map()
	}
	conn.receive(make_reliable_datagram(Uint24(0), Uint24(0), [u8(0xaa)]))!
	conn.receive(make_reliable_datagram(Uint24(1), Uint24(1), [u8(0xbb)]))!
	assert conn.sent_raw.len == 0
	conn.flush_acknowledgements()!
	assert conn.sent_raw.len == 1
	assert conn.sent_raw[0][0] & bit_flag_ack != 0
	mut ack := Acknowledgement{}
	ack.read(conn.sent_raw[0][1..])!
	assert ack.packets == [Uint24(0), Uint24(1)]
}

fn test_conn_chunks_large_acknowledgement_batches_by_mtu() {
	mut conn := &Conn{
		mtu:     64
		packets: chan []u8{cap: 4}
		resend:  new_resend_map()
	}
	mut packets := []Uint24{}
	for i in 0 .. 100 {
		packets << Uint24(i * 2)
	}
	conn.send_ack(packets)!
	assert conn.sent_raw.len > 1
	mut got := []Uint24{}
	for raw in conn.sent_raw {
		assert raw.len <= conn.effective_mtu()
		assert raw[0] & bit_flag_ack != 0
		mut ack := Acknowledgement{}
		ack.read(raw[1..])!
		got << ack.packets
	}
	assert got == packets
}

fn test_conn_resends_only_after_rtt_delay() {
	mut conn := &Conn{
		mtu:    max_mtu_size
		resend: new_resend_map()
	}
	now := time.now()
	conn.resend.delays << DelayRecord{
		at:    now
		delay: 200 * time.millisecond
	}
	conn.resend.add_at(Uint24(1), Packet{
		reliability:   .reliable
		message_index: Uint24(1)
		content:       [u8(0xaa)]
	}, now.add(-100 * time.millisecond))
	conn.check_resend(now)!
	assert conn.sent_raw.len == 0
	conn.check_resend(now.add(250 * time.millisecond))!
	assert conn.sent_raw.len == 1
}

fn test_conn_recovers_from_dropped_datagram_with_nack() {
	mut sender := &Conn{
		mtu:    max_mtu_size
		resend: new_resend_map()
	}
	sender.write([u8(1)])!
	sender.write([u8(2)])!
	sender.write([u8(3)])!
	assert sender.sent_raw.len == 3

	mut receiver := &Conn{
		mtu:          max_mtu_size
		packets:      chan []u8{cap: 4}
		win:          new_datagram_window()
		packet_queue: new_packet_queue()
		resend:       new_resend_map()
	}
	receiver.receive(sender.sent_raw[0])!
	assert (<-receiver.packets) == [u8(1)]

	time.sleep(80 * time.millisecond)
	receiver.receive(sender.sent_raw[2])!
	time.sleep(80 * time.millisecond)
	missing := receiver.win.missing(50 * time.millisecond, time.now())
	receiver.queue_nack(missing)
	receiver.flush_acknowledgements()!
	mut saw_nack := false
	for raw in receiver.sent_raw {
		if raw[0] & bit_flag_nack == 0 {
			continue
		}
		mut nack := Acknowledgement{}
		nack.read(raw[1..])!
		assert Uint24(1) in nack.packets
		sender.handle_nack(raw[1..])!
		saw_nack = true
	}
	assert saw_nack
	assert sender.sent_raw.len == 4

	receiver.receive(sender.sent_raw[3])!
	assert (<-receiver.packets) == [u8(2)]
	assert (<-receiver.packets) == [u8(3)]
}

fn test_conn_latency_tracks_ack_round_trip() {
	mut conn := &Conn{
		mtu:    max_mtu_size
		resend: new_resend_map()
	}
	now := time.now()
	conn.resend.add_at(Uint24(4), Packet{
		reliability:   .reliable
		message_index: Uint24(4)
		content:       [u8(0xaa)]
	}, now.add(-100 * time.millisecond))
	mut ack := Acknowledgement{
		packets: [Uint24(4)]
	}
	mut raw := []u8{}
	ack.write(mut raw, max_mtu_size)
	conn.handle_ack_at(raw, now)!
	assert conn.latency() == 50 * time.millisecond
}

fn test_conn_read_packet_fails_after_close() {
	mut conn := &Conn{
		packets: chan []u8{cap: 1}
	}
	conn.close()!
	conn.read_packet() or {
		assert err.msg().contains('closed')
		return
	}
	assert false, 'read_packet after close should fail'
}

fn test_conn_close_sends_disconnect_notification() {
	mut conn := &Conn{
		mtu: max_mtu_size
	}
	conn.close()!
	assert conn.closing
	assert !conn.closed
	assert conn.sent_raw.len == 1
	mut pk := Packet{}
	pk.read(conn.sent_raw[0][4..])!
	assert pk.content == [message.id_disconnect_notification]
}

fn test_conn_close_finishes_after_disconnect_ack() {
	mut conn := &Conn{
		mtu:             max_mtu_size
		resend:          new_resend_map()
		lifecycle_mutex: sync.new_mutex()
		mutex:           sync.new_mutex()
		ack_mutex:       sync.new_mutex()
		closed_chan:     chan bool{cap: 1}
	}
	conn.close()!
	seq := load_uint24(conn.sent_raw[0][1..])
	mut ack := Acknowledgement{
		packets: [seq]
	}
	mut raw := []u8{}
	ack.write(mut raw, max_mtu_size)
	conn.handle_ack_at(raw, time.now())!
	assert conn.closed
}

fn test_conn_close_finishes_after_drain_timeout() {
	mut conn := &Conn{
		mtu:             max_mtu_size
		resend:          new_resend_map()
		lifecycle_mutex: sync.new_mutex()
		mutex:           sync.new_mutex()
		ack_mutex:       sync.new_mutex()
		closed_chan:     chan bool{cap: 1}
	}
	conn.close()!
	assert !conn.closed
	conn.check_close_drain(time.now().add(close_drain_timeout + time.millisecond))
	assert conn.closed
}

fn test_conn_receive_after_close_is_noop() {
	mut conn := &Conn{
		mtu:          max_mtu_size
		packets:      chan []u8{cap: 1}
		win:          new_datagram_window()
		packet_queue: new_packet_queue()
		resend:       new_resend_map()
	}
	conn.close()!
	conn.sent_raw.clear()
	conn.receive(make_reliable_datagram(Uint24(0), Uint24(0), [u8(0xaa)]))!
	assert conn.sent_raw.len == 0
	select {
		_ := <-conn.packets {
			assert false, 'closed connection delivered packet'
		}
		else {}
	}
}

fn test_conn_reassembles_split_payload_directly() {
	payload := []u8{len: 3000, init: u8(index % 251)}
	mut sender := &Conn{
		mtu: max_mtu_size
	}
	sender.write(payload)!
	assert sender.sent_raw.len == 3
	mut receiver := &Conn{
		mtu:           max_mtu_size
		packets:       chan []u8{cap: 4}
		win:           new_datagram_window()
		packet_queue:  new_packet_queue()
		resend:        new_resend_map()
		last_activity: time.now()
		idle_timeout:  5 * time.second
	}
	for datagram in sender.sent_raw {
		mut pk := Packet{}
		pk.read(datagram[4..])!
		assert pk.split
		assert pk.split_count == 3
		receiver.receive(datagram)!
	}
	assert receiver.splits.len == 0
	select {
		got := <-receiver.packets {
			assert got == payload
		}
		100 * time.millisecond {
			assert false, 'split payload was not reassembled'
		}
	}
}

fn test_conn_idle_timeout_closes_connection() {
	mut conn := &Conn{
		idle_timeout:  20 * time.millisecond
		last_activity: time.now().add(-100 * time.millisecond)
	}
	conn.check_idle_timeout(time.now())
	assert conn.closed
}

fn test_conn_keepalive_sends_connected_ping() {
	mut conn := &Conn{
		mtu: max_mtu_size
	}
	conn.send_keepalive_ping()!
	assert conn.sent_raw.len == 1
	assert conn.sent_raw[0][0] & bit_flag_datagram != 0
}

fn test_conn_close_removes_listener_connection() {
	mut listener := &Listener{
		connections: map[string]&Conn{}
	}
	mut conn := &Conn{
		remote_key: '127.0.0.1:19132'
		listener:   listener
	}
	listener.connections[conn.remote_key] = conn
	conn.close()!
	assert conn.remote_key !in listener.connections
}

fn test_conn_remote_disconnect_closes_without_echo() {
	mut conn := &Conn{
		mtu:             max_mtu_size
		packets:         chan []u8{cap: 4}
		connected:       chan bool{cap: 1}
		lifecycle_mutex: sync.new_mutex()
		ack_mutex:       sync.new_mutex()
		mutex:           sync.new_mutex()
		closed_chan:     chan bool{cap: 1}
	}
	conn.handle_packet([message.id_disconnect_notification], .reliable_ordered)!
	assert conn.is_closed()
	assert conn.sent_raw.len == 0
}

fn test_conn_duplicate_new_incoming_connection_does_not_block() {
	mut conn := &Conn{
		is_server:       true
		packets:         chan []u8{cap: 4}
		connected:       chan bool{cap: 1}
		lifecycle_mutex: sync.new_mutex()
		ack_mutex:       sync.new_mutex()
		mutex:           sync.new_mutex()
		closed_chan:     chan bool{cap: 1}
	}
	data := message.NewIncomingConnection{
		ping_time: timestamp()
		pong_time: timestamp()
	}.encode()
	conn.handle_packet(data, .reliable_ordered)!
	conn.handle_packet(data, .reliable_ordered)!
	assert conn.connected.len == 1
}

fn test_listener_accept_timeout_returns_closed_error_when_closed() {
	mut listener := &Listener{
		incoming: chan &Conn{cap: 1}
	}
	listener.incoming.close()
	listener.accept_timeout(10 * time.millisecond) or {
		assert err.msg() == 'listener closed'
		return
	}
	assert false, 'accept_timeout should fail on closed listener'
}

fn test_conn_public_addresses_and_latency_defaults() {
	mut conn := &Conn{
		remote_key:    '127.0.0.1:19132'
		mtu:           max_mtu_size
		last_activity: time.now()
	}
	assert conn.remote_addr() == '127.0.0.1:19132'
	assert conn.local_addr() == ''
	assert conn.latency() == 0
}

fn test_conn_rejects_packet_queue_window_over_limit() {
	mut conn := &Conn{
		packets:      chan []u8{cap: 4}
		packet_queue: new_packet_queue()
	}
	conn.receive_packet(Packet{
		reliability: .reliable_ordered
		order_index: Uint24(2050)
		content:     [u8(0xaa)]
	}) or {
		assert err.msg().contains('window')
		return
	}
	assert false, 'packet queue window above limit should fail'
}

fn test_conn_rejects_too_many_concurrent_splits() {
	mut conn := &Conn{
		packet_queue: new_packet_queue()
		splits:       map[u16][][]u8{}
	}
	for i in 0 .. 17 {
		conn.splits[u16(i)] = [][]u8{len: 2}
	}
	conn.receive_split_packet(Packet{
		reliability: .reliable_ordered
		split:       true
		split_count: 2
		split_index: 0
		split_id:    18
		content:     [u8(0xaa)]
	}) or {
		assert err.msg().contains('concurrent')
		return
	}
	assert false, 'too many concurrent splits should fail'
}

fn test_listener_accepts_cookie_from_previous_salt() {
	mut listener := listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	mut udp := net.dial_udp(listener.addr()) or { panic(err) }
	udp.set_read_timeout(500 * time.millisecond)
	defer {
		udp.close() or {}
	}
	udp.write(message.OpenConnectionRequest1{
		client_protocol: protocol_version
		mtu:             max_mtu_size
	}.encode()) or { panic(err) }
	mut buf := []u8{len: 1500}
	n1, _ := udp.read(mut buf) or {
		assert false, 'previous salt request1 read failed: ${err.msg()}'
		return
	}
	reply1 := message.decode_open_connection_reply_1(buf[1..n1]) or { panic(err) }
	listener.rotate_cookie_salt()
	udp.write(message.OpenConnectionRequest2{
		server_address:      addr_port_from_string(listener.addr()) or { panic(err) }
		mtu:                 reply1.mtu
		client_guid:         -123
		server_has_security: true
		cookie:              reply1.cookie
	}.encode()) or { panic(err) }
	n2, _ := udp.read(mut buf) or {
		assert false, 'previous salt request2 read failed: ${err.msg()}'
		return
	}
	assert n2 > 0
	assert buf[0] == message.id_open_connection_reply_2
}

fn test_listener_blocks_invalid_cookie_and_unblocks_after_duration() {
	mut listener := ListenConfig{
		block_duration: 150 * time.millisecond
	}.listen('127.0.0.1:0') or { panic(err) }
	defer {
		listener.close() or {}
	}
	mut udp := net.dial_udp(listener.addr()) or { panic(err) }
	defer {
		udp.close() or {}
	}
	udp.set_read_timeout(500 * time.millisecond)
	udp.write(message.OpenConnectionRequest1{
		client_protocol: protocol_version
		mtu:             max_mtu_size
	}.encode()) or { panic(err) }
	mut buf := []u8{len: 1500}
	n1, _ := udp.read(mut buf) or {
		assert false, 'block invalid cookie request1 read failed: ${err.msg()}'
		return
	}
	reply1 := message.decode_open_connection_reply_1(buf[1..n1]) or { panic(err) }
	udp.write(message.OpenConnectionRequest2{
		server_address:      addr_port_from_string(listener.addr()) or { panic(err) }
		mtu:                 reply1.mtu
		client_guid:         -123
		server_has_security: true
		cookie:              0
	}.encode()) or { panic(err) }

	udp.write(message.OpenConnectionRequest1{
		client_protocol: protocol_version
		mtu:             max_mtu_size
	}.encode()) or { panic(err) }
	time.sleep(320 * time.millisecond)
	udp.set_read_timeout(500 * time.millisecond)
	udp.write(message.OpenConnectionRequest1{
		client_protocol: protocol_version
		mtu:             max_mtu_size
	}.encode()) or { panic(err) }
	n3, _ := udp.read(mut buf) or {
		assert false, 'block expiry request1 read failed: ${err.msg()}'
		return
	}
	assert n3 > 0
	assert buf[0] == message.id_open_connection_reply_1
}
