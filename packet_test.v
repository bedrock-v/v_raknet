module raknet

fn test_reliable_ordered_packet() {
	pk := Packet{
		reliability:   .reliable_ordered
		message_index: Uint24(1)
		order_index:   Uint24(2)
		content:       [u8(0xfe)]
	}
	mut out := []u8{}
	pk.write(mut out)
	assert out == [u8(0x60), 0x00, 0x08, 0x01, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xfe]

	mut got := Packet{}
	n := got.read(out)!
	assert n == out.len
	assert got.reliability == .reliable_ordered
	assert got.message_index == Uint24(1)
	assert got.order_index == Uint24(2)
	assert got.content == [u8(0xfe)]
}

fn test_split_mtu() {
	parts := split_packet_content([]u8{len: 1000, init: u8(index % 251)}, 500)
	assert parts.len == 3
	assert parts[0].len == 476
	assert parts[1].len == 476
	assert parts[2].len == 48
}

fn test_split_empty() {
	assert split_packet_content([]u8{}, 500).len == 0
}

fn test_packet_zero_content() {
	mut pk := Packet{}
	mut failed := false
	pk.read([u8(0x60), 0x00, 0x00]) or {
		failed = true
		assert err.msg().contains('cannot be 0')
	}
	assert failed
}

fn test_reliability_matrix() {
	cases := [
		Packet{
			reliability: .unreliable
			content:     [u8(0x01)]
		},
		Packet{
			reliability:    .unreliable_sequenced
			sequence_index: Uint24(3)
			order_index:    Uint24(4)
			content:        [u8(0x02)]
		},
		Packet{
			reliability:   .reliable
			message_index: Uint24(5)
			content:       [u8(0x03)]
		},
		Packet{
			reliability:   .reliable_ordered
			message_index: Uint24(6)
			order_index:   Uint24(7)
			content:       [u8(0x04)]
		},
		Packet{
			reliability:    .reliable_sequenced
			message_index:  Uint24(8)
			sequence_index: Uint24(9)
			order_index:    Uint24(10)
			content:        [u8(0x05)]
		},
	]
	for pk in cases {
		mut out := []u8{}
		pk.write(mut out)
		mut got := Packet{}
		n := got.read(out)!
		assert n == out.len
		assert got.reliability == pk.reliability
		assert got.message_index == pk.message_index
		assert got.sequence_index == pk.sequence_index
		assert got.order_index == pk.order_index
		assert got.content == pk.content
	}
}

fn test_unknown_reliability() {
	mut pk := Packet{}
	pk.read([u8(0xe0), 0x00, 0x08, 0xaa]) or {
		assert err.msg().contains('reliability')
		return
	}
	assert false, 'packet with unknown reliability should fail'
}

fn test_reliability_indexes() {
	mut conn := &Conn{
		mtu: max_mtu_size
	}
	conn.write_unreliable([u8(0xaa)])!
	conn.write_reliable([u8(0xbb)])!
	conn.write_reliable_ordered([u8(0xcc)])!
	conn.write_unreliable_sequenced([u8(0xdd)])!
	conn.write_reliable_sequenced([u8(0xee)])!

	mut unreliable := Packet{}
	unreliable.read(conn.sent_raw[0][4..])!
	assert unreliable.reliability == .unreliable
	assert unreliable.message_index == Uint24(0)
	assert unreliable.sequence_index == Uint24(0)
	assert unreliable.order_index == Uint24(0)

	mut reliable := Packet{}
	reliable.read(conn.sent_raw[1][4..])!
	assert reliable.reliability == .reliable
	assert reliable.message_index == Uint24(0)
	assert reliable.sequence_index == Uint24(0)
	assert reliable.order_index == Uint24(0)

	mut ordered := Packet{}
	ordered.read(conn.sent_raw[2][4..])!
	assert ordered.reliability == .reliable_ordered
	assert ordered.message_index == Uint24(1)
	assert ordered.sequence_index == Uint24(0)
	assert ordered.order_index == Uint24(0)

	mut sequenced := Packet{}
	sequenced.read(conn.sent_raw[3][4..])!
	assert sequenced.reliability == .unreliable_sequenced
	assert sequenced.message_index == Uint24(0)
	assert sequenced.sequence_index == Uint24(0)
	assert sequenced.order_index == Uint24(1)

	mut reliable_sequenced := Packet{}
	reliable_sequenced.read(conn.sent_raw[4][4..])!
	assert reliable_sequenced.reliability == .reliable_sequenced
	assert reliable_sequenced.message_index == Uint24(2)
	assert reliable_sequenced.sequence_index == Uint24(1)
	assert reliable_sequenced.order_index == Uint24(2)
}
