module raknet

import net
import sync
import time
import message

const resend_min_delay = 50 * time.millisecond
const resend_max_delay = 2 * time.second

@[heap]
pub struct Conn {
mut:
	udp                 &net.UdpConn = unsafe { nil }
	remote              net.Addr
	remote_key          string
	mtu                 u16
	seq                 Uint24
	message_idx         Uint24
	sequence_idx        Uint24
	order_idx           Uint24
	split_id            u32
	packets             chan []u8
	connected           chan bool
	splits              map[u16][][]u8
	resend              ResendMap
	win                 DatagramWindow
	packet_queue        PacketQueue
	pending_ack         []Uint24
	pending_nack        []Uint24
	sent_raw            [][]u8
	rtt                 time.Duration
	last_activity       time.Time
	idle_timeout        time.Duration
	keepalive_interval  time.Duration
	read_deadline       time.Time
	write_deadline      time.Time
	read_timeout        time.Duration
	is_server           bool
	listener            &Listener   = unsafe { nil }
	mutex               &sync.Mutex = sync.new_mutex()
	ack_mutex           &sync.Mutex = sync.new_mutex()
	lifecycle_mutex     &sync.Mutex = sync.new_mutex()
	closed_chan         chan bool   = chan bool{cap: 1}
	connected_confirmed bool
	closing_deadline    time.Time
	closing             bool
	closed              bool
}

fn new_conn(mut udp net.UdpConn, remote net.Addr, mtu u16, is_server bool, listener &Listener) &Conn {
	mut c := &Conn{
		udp:                unsafe { &udp }
		remote:             remote
		remote_key:         normalise_addr_string(remote.str())
		mtu:                clamp_mtu(mtu, min_mtu_size)
		packets:            chan []u8{cap: 256}
		connected:          chan bool{cap: 1}
		splits:             map[u16][][]u8{}
		resend:             new_resend_map()
		win:                new_datagram_window()
		packet_queue:       new_packet_queue()
		pending_ack:        []Uint24{}
		pending_nack:       []Uint24{}
		sent_raw:           [][]u8{}
		last_activity:      time.now()
		idle_timeout:       5 * time.second
		keepalive_interval: 250 * time.millisecond
		is_server:          is_server
		listener:           listener
		mutex:              sync.new_mutex()
		ack_mutex:          sync.new_mutex()
		lifecycle_mutex:    sync.new_mutex()
		closed_chan:        chan bool{cap: 1}
	}
	spawn c.ack_loop()
	return c
}

pub fn (mut c Conn) remote_addr() string {
	if c.remote_key != '' {
		return c.remote_key
	}
	return c.remote.str()
}

pub fn (mut c Conn) local_addr() string {
	if c.udp == unsafe { nil } {
		return ''
	}
	return c.udp.sock.address() or { return '' }.str()
}

pub fn (mut c Conn) latency() time.Duration {
	c.lifecycle_mutex.lock()
	rtt := c.rtt
	c.lifecycle_mutex.unlock()
	return rtt / 2
}

pub fn (mut c Conn) set_read_deadline(deadline time.Time) {
	c.lifecycle_mutex.lock()
	c.read_deadline = deadline
	c.lifecycle_mutex.unlock()
}

pub fn (mut c Conn) set_write_deadline(deadline time.Time) {
	c.lifecycle_mutex.lock()
	c.write_deadline = deadline
	c.lifecycle_mutex.unlock()
}

pub fn (mut c Conn) set_deadline(deadline time.Time) {
	c.lifecycle_mutex.lock()
	c.read_deadline = deadline
	c.write_deadline = deadline
	c.lifecycle_mutex.unlock()
}

pub fn (mut c Conn) set_read_timeout(timeout time.Duration) {
	c.lifecycle_mutex.lock()
	c.read_timeout = timeout
	c.lifecycle_mutex.unlock()
}

pub fn (mut c Conn) set_write_timeout(timeout time.Duration) {
	c.lifecycle_mutex.lock()
	if timeout <= 0 || timeout == net.infinite_timeout {
		c.write_deadline = time.Time{}
	} else {
		c.write_deadline = time.now().add(timeout)
	}
	c.lifecycle_mutex.unlock()
}

pub fn (mut c Conn) set_idle_timeout(timeout time.Duration) {
	c.lifecycle_mutex.lock()
	c.idle_timeout = timeout
	c.lifecycle_mutex.unlock()
}

pub fn (mut c Conn) set_keepalive_interval(interval time.Duration) {
	c.lifecycle_mutex.lock()
	c.keepalive_interval = interval
	c.lifecycle_mutex.unlock()
}

pub fn (mut c Conn) write(data []u8) !int {
	if data.len == 0 {
		return error('cannot write empty packet')
	}
	return c.write_with_reliability(data, .reliable_ordered)
}

pub fn (mut c Conn) write_reliable_ordered(data []u8) !int {
	return c.write(data)
}

pub fn (mut c Conn) write_reliable(data []u8) !int {
	if data.len == 0 {
		return error('cannot write empty packet')
	}
	return c.write_with_reliability(data, .reliable)
}

pub fn (mut c Conn) write_unreliable(data []u8) !int {
	if data.len == 0 {
		return error('cannot write empty packet')
	}
	return c.write_with_reliability(data, .unreliable)
}

pub fn (mut c Conn) write_unreliable_sequenced(data []u8) !int {
	if data.len == 0 {
		return error('cannot write empty packet')
	}
	return c.write_with_reliability(data, .unreliable_sequenced)
}

pub fn (mut c Conn) write_reliable_sequenced(data []u8) !int {
	if data.len == 0 {
		return error('cannot write empty packet')
	}
	return c.write_with_reliability(data, .reliable_sequenced)
}

fn (mut c Conn) write_with_reliability(data []u8, reliability Reliability) !int {
	return c.write_with_reliability_internal(data, reliability, false)
}

fn (mut c Conn) write_with_reliability_internal(data []u8, reliability Reliability, allow_closing bool) !int {
	if !allow_closing && c.is_closing_or_closed() {
		return err_connection_closed
	}
	if c.write_deadline_expired(time.now()) {
		return err_write_deadline_exceeded
	}
	c.mutex.lock()
	defer {
		c.mutex.unlock()
	}
	fragments := split_packet_content(data, c.effective_mtu())
	order_index := if reliability.sequenced_or_ordered() { c.order_idx.inc() } else { Uint24(0) }
	sequence_index := if reliability.sequenced() { c.sequence_idx.inc() } else { Uint24(0) }
	split_id := u16(c.split_id)
	if fragments.len > 1 {
		c.split_id++
	}
	for split_index, content in fragments {
		mut pk := Packet{
			reliability:    reliability
			message_index:  if reliability.reliable() { c.message_idx.inc() } else { Uint24(0) }
			sequence_index: sequence_index
			order_index:    order_index
			content:        content.clone()
			split:          fragments.len > 1
			split_count:    u32(fragments.len)
			split_index:    u32(split_index)
			split_id:       split_id
		}
		c.send_datagram_locked(mut pk)!
	}
	return data.len
}

fn (c Conn) effective_mtu() u16 {
	if c.mtu == 0 {
		return max_mtu_size - 28
	}
	return c.mtu - 28
}

fn (mut c Conn) send_datagram(mut pk Packet) ! {
	c.mutex.lock()
	defer {
		c.mutex.unlock()
	}
	c.send_datagram_locked(mut pk)!
}

fn (mut c Conn) send_datagram_locked(mut pk Packet) ! {
	mut out := []u8{}
	out << (bit_flag_datagram | bit_flag_needs_b_and_as)
	seq := c.seq.inc()
	write_uint24(mut out, seq)
	pk.write(mut out)
	if pk.reliability.reliable() {
		c.resend.add(seq, pk)
	}
	c.write_raw(out)!
}

fn (mut c Conn) write_raw(data []u8) ! {
	if c.udp == unsafe { nil } {
		c.sent_raw << data.clone()
		return
	}
	if c.is_server {
		c.udp.write_to(c.remote, data) or {
			if c.is_closing_or_closed() {
				return
			}
			return err
		}
	} else {
		c.udp.write(data) or {
			if c.is_closing_or_closed() {
				return
			}
			return err
		}
	}
}

pub fn (mut c Conn) read(mut buf []u8) !int {
	if c.is_closed() {
		return err_connection_closed
	}
	mut data := []u8{}
	wait, has_timeout := c.read_wait_timeout(time.now())
	if has_timeout {
		select {
			packet := <-c.packets {
				data = packet.clone()
			}
			_ := <-c.closed_chan {
				return err_connection_closed
			}
			1 * wait {
				return err_read_deadline_exceeded
			}
		}
	} else {
		select {
			packet := <-c.packets {
				data = packet.clone()
			}
			_ := <-c.closed_chan {
				return err_connection_closed
			}
		}
	}
	if data.len > buf.len {
		return error('buffer too small')
	}
	copy(mut buf, data)
	return data.len
}

pub fn (mut c Conn) read_packet() ![]u8 {
	if c.is_closed() {
		return err_connection_closed
	}
	wait, has_timeout := c.read_wait_timeout(time.now())
	if has_timeout {
		select {
			packet := <-c.packets {
				return packet.clone()
			}
			_ := <-c.closed_chan {
				return err_connection_closed
			}
			1 * wait {
				return err_read_deadline_exceeded
			}
		}
	} else {
		select {
			packet := <-c.packets {
				return packet.clone()
			}
			_ := <-c.closed_chan {
				return err_connection_closed
			}
		}
	}
	return err_connection_closed
}

pub fn (mut c Conn) close() ! {
	mut should_send_disconnect := false
	c.lifecycle_mutex.lock()
	if c.closed || c.closing {
		c.lifecycle_mutex.unlock()
		return
	}
	c.closing = true
	c.closing_deadline = time.now().add(close_drain_timeout)
	if !c.closed_chan.closed {
		c.closed_chan <- true or {}
		c.closed_chan.close()
	}
	should_send_disconnect = c.mtu != 0
	c.lifecycle_mutex.unlock()
	if should_send_disconnect {
		c.write_with_reliability_internal([message.id_disconnect_notification], .reliable_ordered,
			true) or {}
		c.flush_acknowledgements() or {}
		c.check_close_drain(time.now())
	} else {
		c.close_immediately()
	}
	return
}

fn (mut c Conn) is_closed() bool {
	c.lifecycle_mutex.lock()
	closed := c.closed
	c.lifecycle_mutex.unlock()
	return closed
}

fn (mut c Conn) is_closing_or_closed() bool {
	c.lifecycle_mutex.lock()
	closed := c.closed || c.closing
	c.lifecycle_mutex.unlock()
	return closed
}

fn (mut c Conn) is_closing() bool {
	c.lifecycle_mutex.lock()
	closing := c.closing && !c.closed
	c.lifecycle_mutex.unlock()
	return closing
}

fn (mut c Conn) is_open_for_receive() bool {
	c.lifecycle_mutex.lock()
	closed := c.closed
	c.lifecycle_mutex.unlock()
	return !closed
}

fn (mut c Conn) mark_activity(now time.Time) {
	c.lifecycle_mutex.lock()
	c.last_activity = now
	c.lifecycle_mutex.unlock()
}

fn (mut c Conn) last_activity_at() time.Time {
	c.lifecycle_mutex.lock()
	last := c.last_activity
	c.lifecycle_mutex.unlock()
	return last
}

fn (mut c Conn) set_rtt(rtt time.Duration) {
	c.lifecycle_mutex.lock()
	c.rtt = rtt
	c.lifecycle_mutex.unlock()
}

fn (mut c Conn) rtt_value() time.Duration {
	c.lifecycle_mutex.lock()
	rtt := c.rtt
	c.lifecycle_mutex.unlock()
	return rtt
}

fn (mut c Conn) keepalive_interval_value() time.Duration {
	c.lifecycle_mutex.lock()
	v := c.keepalive_interval
	c.lifecycle_mutex.unlock()
	return v
}

fn (mut c Conn) idle_timeout_value() time.Duration {
	c.lifecycle_mutex.lock()
	v := c.idle_timeout
	c.lifecycle_mutex.unlock()
	return v
}

fn (mut c Conn) read_wait_timeout(now time.Time) (time.Duration, bool) {
	c.lifecycle_mutex.lock()
	deadline := c.read_deadline
	timeout := c.read_timeout
	c.lifecycle_mutex.unlock()
	mut has_timeout := false
	mut wait := time.Duration(0)
	if timeout > 0 && timeout != net.infinite_timeout {
		wait = timeout
		has_timeout = true
	}
	if deadline != time.Time{} {
		remaining := deadline - now
		if remaining <= 0 {
			return time.Duration(0), true
		}
		if !has_timeout || remaining < wait {
			wait = remaining
		}
		has_timeout = true
	}
	return wait, has_timeout
}

fn (mut c Conn) write_deadline_expired(now time.Time) bool {
	c.lifecycle_mutex.lock()
	deadline := c.write_deadline
	c.lifecycle_mutex.unlock()
	if deadline != time.Time{} && now >= deadline {
		return true
	}
	return false
}

fn (mut c Conn) close_signal() chan bool {
	c.lifecycle_mutex.lock()
	ch := c.closed_chan
	c.lifecycle_mutex.unlock()
	return ch
}

fn (mut c Conn) close_immediately() {
	c.lifecycle_mutex.lock()
	if c.closed {
		c.lifecycle_mutex.unlock()
		return
	}
	unsafe {
		c.closing = true
		c.closed = true
	}
	if !c.closed_chan.closed {
		c.closed_chan <- true or {}
		c.closed_chan.close()
	}
	c.lifecycle_mutex.unlock()
	if c.listener != unsafe { nil } {
		mut listener := c.listener
		listener.delete_conn_if_same(c.remote_key, c)
	} else if c.udp != unsafe { nil } {
		mut udp := c.udp
		udp.close() or {}
	}
	return
}

fn (mut c Conn) check_close_drain(now time.Time) {
	c.lifecycle_mutex.lock()
	if c.closed || !c.closing {
		c.lifecycle_mutex.unlock()
		return
	}
	deadline := c.closing_deadline
	c.lifecycle_mutex.unlock()
	c.mutex.lock()
	resend_empty := c.resend.len() == 0
	c.mutex.unlock()
	if resend_empty || now >= deadline {
		c.close_immediately()
	}
}

fn (mut c Conn) mark_connected_once() bool {
	c.lifecycle_mutex.lock()
	if c.connected_confirmed {
		c.lifecycle_mutex.unlock()
		return false
	}
	c.connected_confirmed = true
	c.lifecycle_mutex.unlock()
	return true
}

fn (mut c Conn) is_connected_confirmed() bool {
	c.lifecycle_mutex.lock()
	connected := c.connected_confirmed
	c.lifecycle_mutex.unlock()
	return connected
}

fn (mut c Conn) receive(data []u8) ! {
	if !c.is_open_for_receive() {
		return
	}
	if data.len == 0 {
		return
	}
	c.mark_activity(time.now())
	if data[0] & bit_flag_ack != 0 {
		c.handle_ack(data[1..])!
		return
	}
	if data[0] & bit_flag_nack != 0 {
		c.handle_nack(data[1..])!
		return
	}
	if c.is_closing_or_closed() {
		return
	}
	if data[0] & bit_flag_datagram == 0 {
		return
	}
	if data.len < 4 {
		return error('datagram too small')
	}
	seq := safe_load_uint24(data[1..])!
	if !c.win.add(seq) {
		return
	}
	c.queue_ack(seq)
	c.mutex.lock()
	rtt := c.resend.rtt(time.now())
	c.mutex.unlock()
	if c.win.shift() == 0 {
		missing := c.win.missing(rtt + rtt / 2, time.now())
		if missing.len > 0 {
			c.queue_nack(missing)
		}
	}
	mut offset := 4
	for offset < data.len {
		mut pk := Packet{}
		n := pk.read(data[offset..])!
		offset += n
		if pk.split {
			c.receive_split_packet(pk)!
		} else {
			c.receive_packet(pk)!
		}
	}
}

fn (mut c Conn) queue_ack(seq Uint24) {
	c.ack_mutex.lock()
	append_unique_uint24(mut c.pending_ack, seq)
	c.ack_mutex.unlock()
}

fn (mut c Conn) queue_nack(seqs []Uint24) {
	c.ack_mutex.lock()
	for seq in seqs {
		append_unique_uint24(mut c.pending_nack, seq)
	}
	c.ack_mutex.unlock()
}

fn (mut c Conn) flush_acknowledgements() ! {
	c.ack_mutex.lock()
	ack_packets := c.pending_ack.clone()
	nack_packets := c.pending_nack.clone()
	c.pending_ack.clear()
	c.pending_nack.clear()
	c.ack_mutex.unlock()
	if ack_packets.len > 0 {
		c.send_ack(ack_packets)!
	}
	if nack_packets.len > 0 {
		c.send_nack(nack_packets)!
	}
}

fn (mut c Conn) send_ack(packets []Uint24) ! {
	mut ack := Acknowledgement{
		packets: packets.clone()
	}
	for ack.packets.len > 0 {
		mut out := []u8{}
		out << (bit_flag_ack | bit_flag_datagram)
		n := ack.write(mut out, c.effective_mtu())
		if n == 0 {
			return error('acknowledgement does not fit mtu')
		}
		ack.packets = ack.packets[n..].clone()
		c.write_raw(out)!
	}
}

fn (mut c Conn) send_nack(packets []Uint24) ! {
	mut nack := Acknowledgement{
		packets: packets.clone()
	}
	for nack.packets.len > 0 {
		mut out := []u8{}
		out << (bit_flag_nack | bit_flag_datagram)
		n := nack.write(mut out, c.effective_mtu())
		if n == 0 {
			return error('negative acknowledgement does not fit mtu')
		}
		nack.packets = nack.packets[n..].clone()
		c.write_raw(out)!
	}
}

fn (mut c Conn) handle_ack(data []u8) ! {
	c.handle_ack_at(data, time.now())!
}

fn (mut c Conn) handle_ack_at(data []u8, now time.Time) ! {
	mut ack := Acknowledgement{}
	ack.read(data)!
	c.mutex.lock()
	for seq in ack.packets {
		c.resend.acknowledge_at(seq, now)
	}
	rtt := c.resend.rtt(now)
	c.mutex.unlock()
	c.set_rtt(rtt)
	c.check_close_drain(now)
}

fn (mut c Conn) handle_nack(data []u8) ! {
	mut nack := Acknowledgement{}
	nack.read(data)!
	c.mutex.lock()
	defer {
		c.mutex.unlock()
	}
	for seq in nack.packets {
		mut pk, ok := c.resend.retransmit(seq)
		if ok {
			c.send_datagram_locked(mut pk)!
		}
	}
}

fn (mut c Conn) ack_loop() {
	mut last_keepalive := time.now()
	mut tick := 0
	for !c.is_closed() {
		time.sleep(50 * time.millisecond)
		c.flush_acknowledgements() or {}
		now := time.now()
		tick++
		if tick % 6 == 0 {
			c.check_resend(now) or {}
		}
		c.check_close_drain(now)
		if c.is_closed() {
			continue
		}
		keepalive_interval := c.keepalive_interval_value()
		if keepalive_interval > 0 && now - last_keepalive >= keepalive_interval {
			c.send_keepalive_ping() or {}
			last_keepalive = now
		}
		c.check_idle_timeout(now)
	}
}

fn (mut c Conn) check_idle_timeout(now time.Time) {
	last_activity := c.last_activity_at()
	idle_timeout := c.idle_timeout_value()
	if c.is_closed() || idle_timeout <= 0 || last_activity.unix() == 0 {
		return
	}
	if now - last_activity > idle_timeout {
		c.close() or {}
	}
}

fn (mut c Conn) send_keepalive_ping() ! {
	if c.is_closing_or_closed() {
		return err_connection_closed
	}
	c.write_with_reliability(message.ConnectedPing{
		ping_time: timestamp()
	}.encode(), .unreliable)!
}

fn (mut c Conn) check_resend(now time.Time) ! {
	c.mutex.lock()
	defer {
		c.mutex.unlock()
	}
	rtt := c.resend.rtt(now)
	delay := clamp_resend_delay(rtt + rtt / 2)
	c.set_rtt(rtt)
	for seq in c.resend.due(now, delay) {
		mut pk, ok := c.resend.retransmit_at(seq, now)
		if ok {
			c.send_datagram_locked(mut pk)!
		}
	}
}

fn append_unique_uint24(mut values []Uint24, value Uint24) {
	if value in values {
		return
	}
	values << value
}

fn clamp_resend_delay(delay time.Duration) time.Duration {
	if delay < resend_min_delay {
		return resend_min_delay
	}
	if delay > resend_max_delay {
		return resend_max_delay
	}
	return delay
}

fn (mut c Conn) receive_split_packet(pk Packet) ! {
	if pk.split_count == 0 || pk.split_count > max_split_count {
		return error('invalid split packet count')
	}
	if pk.split_index >= pk.split_count {
		return error('split packet index out of range')
	}
	if pk.split_id !in c.splits && c.splits.len >= max_concurrent_splits {
		return error('maximum concurrent splits reached')
	}
	mut fragments := c.splits[pk.split_id] or { [][]u8{len: int(pk.split_count)} }
	fragments[int(pk.split_index)] = pk.content.clone()
	c.splits[pk.split_id] = fragments
	for fragment in fragments {
		if fragment.len == 0 {
			return
		}
	}
	mut content := []u8{}
	for fragment in fragments {
		content << fragment
	}
	c.splits.delete(pk.split_id)
	mut full := pk.clone()
	full.content = content
	full.split = false
	c.receive_packet(full)!
}

fn (mut c Conn) receive_packet(pk Packet) ! {
	if pk.reliability != .reliable_ordered {
		c.handle_packet(pk.content, pk.reliability)!
		return
	}
	if !c.packet_queue.put(pk.order_index, pk.content) {
		return
	}
	if c.packet_queue.window_size() > Uint24(max_window_size) {
		c.close_immediately()
		return error('packet queue window size is too big')
	}
	for content in c.packet_queue.fetch() {
		c.handle_packet(content, pk.reliability)!
	}
}

fn (mut c Conn) handle_packet(data []u8, reliability Reliability) ! {
	if data.len == 0 {
		return
	}
	match data[0] {
		message.id_connection_request {
			if !c.is_server {
				return
			}
			req := message.decode_connection_request(data[1..])!
			addr := addr_port_from_string(c.remote.str()) or { message.AddrPort{} }
			c.write(message.ConnectionRequestAccepted{
				client_address: addr
				ping_time:      req.request_time
				pong_time:      timestamp()
			}.encode())!
		}
		message.id_connection_request_accepted {
			if c.is_server {
				return
			}
			accepted := message.decode_connection_request_accepted(data[1..])!
			c.write(message.NewIncomingConnection{
				server_address: accepted.client_address
				ping_time:      accepted.pong_time
				pong_time:      timestamp()
			}.encode())!
			if c.mark_connected_once() {
				c.connected <- true or {}
			}
		}
		message.id_new_incoming_connection {
			if !c.is_server {
				return
			}
			_ := message.decode_new_incoming_connection(data[1..])!
			if c.mark_connected_once() {
				c.connected <- true or {}
				if c.listener != unsafe { nil } {
					c.listener.queue_incoming(c)
				}
			}
		}
		message.id_disconnect_notification {
			c.close_immediately()
		}
		message.id_connected_ping {
			if data.len == 9 {
				ping_packet := message.decode_connected_ping(data[1..])!
				c.write_with_reliability(message.ConnectedPong{
					ping_time: ping_packet.ping_time
					pong_time: timestamp()
				}.encode(), .unreliable)!
			} else if reliability != .reliable_ordered {
				return error('malformed connected ping')
			} else {
				c.packets <- data.clone()
			}
		}
		message.id_connected_pong {
			if data.len == 17 {
				_ := message.decode_connected_pong(data[1..])!
			} else if reliability != .reliable_ordered {
				return error('malformed connected pong')
			} else {
				c.packets <- data.clone()
			}
		}
		message.id_detect_lost_connections {
			c.send_keepalive_ping()!
		}
		else {
			c.packets <- data.clone()
		}
	}
}

fn wait_connected(conn &Conn, timeout time.Duration) ! {
	select {
		_ := <-conn.connected {
			return
		}
		1 * timeout {
			return err_connection_timed_out
		}
	}
}
