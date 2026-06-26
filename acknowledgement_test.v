module raknet

fn test_ack_ranges_roundtrip() {
	mut ack := Acknowledgement{
		packets: [Uint24(7), 1, 3, 2]
	}
	mut out := []u8{}
	n := ack.write(mut out, 1200)
	assert n == 4
	assert out == [u8(0x00), 0x02, 0x00, 0x01, 0x00, 0x00, 0x03, 0x00, 0x00, 0x01, 0x07, 0x00,
		0x00]

	mut decoded := Acknowledgement{}
	decoded.read(out)!
	assert decoded.packets == [Uint24(1), 2, 3, 7]
}

fn test_ack_read_limit() {
	mut data := []u8{}
	data << [u8(0), 1] // one range record
	data << packet_range
	write_uint24(mut data, Uint24(0))
	write_uint24(mut data, Uint24(8193))

	mut decoded := Acknowledgement{}
	mut failed := false
	decoded.read(data) or {
		failed = true
		assert err.msg().contains('maximum amount')
	}
	assert failed
}

fn test_ack_mtu_boundary() {
	mut ack := Acknowledgement{
		packets: [Uint24(1), 10, 20, 30, 40, 50]
	}
	mut out := []u8{}
	n := ack.write(mut out, 12)
	assert n == 2
	assert out.len <= 12
}
