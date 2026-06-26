module raknet

import encoding.binary

const bit_flag_datagram = u8(0x80)
const bit_flag_ack = u8(0x40)
const bit_flag_nack = u8(0x20)
const bit_flag_needs_b_and_as = u8(0x04)
const split_flag = u8(0x10)

enum Reliability {
	unreliable
	unreliable_sequenced
	reliable
	reliable_ordered
	reliable_sequenced
}

fn (r Reliability) reliable() bool {
	return r == .reliable || r == .reliable_ordered || r == .reliable_sequenced
}

fn (r Reliability) sequenced() bool {
	return r == .unreliable_sequenced || r == .reliable_sequenced
}

fn (r Reliability) sequenced_or_ordered() bool {
	return r.sequenced() || r == .reliable_ordered
}

struct Packet {
mut:
	reliability    Reliability
	message_index  Uint24
	sequence_index Uint24
	order_index    Uint24
	content        []u8
	split          bool
	split_count    u32
	split_index    u32
	split_id       u16
}

fn (pk Packet) clone() Packet {
	return Packet{
		reliability:    pk.reliability
		message_index:  pk.message_index
		sequence_index: pk.sequence_index
		order_index:    pk.order_index
		content:        pk.content.clone()
		split:          pk.split
		split_count:    pk.split_count
		split_index:    pk.split_index
		split_id:       pk.split_id
	}
}

fn (pk Packet) write(mut out []u8) {
	mut header := u8(u32(int(pk.reliability)) << 5)
	if pk.split {
		header |= split_flag
	}
	out << header
	mut len_buf := []u8{len: 2}
	binary.big_endian_put_u16(mut len_buf, u16(pk.content.len) << 3)
	out << len_buf
	if pk.reliability.reliable() {
		write_uint24(mut out, pk.message_index)
	}
	if pk.reliability.sequenced() {
		write_uint24(mut out, pk.sequence_index)
	}
	if pk.reliability.sequenced_or_ordered() {
		write_uint24(mut out, pk.order_index)
		out << u8(0)
	}
	if pk.split {
		mut split_buf := []u8{len: 10}
		binary.big_endian_put_u32_at(mut split_buf, pk.split_count, 0)
		binary.big_endian_put_u16_at(mut split_buf, pk.split_id, 4)
		binary.big_endian_put_u32_at(mut split_buf, pk.split_index, 6)
		out << split_buf
	}
	out << pk.content
}

fn (mut pk Packet) read(b []u8) !int {
	if b.len < 3 {
		return error('unexpected eof reading packet header')
	}
	header := b[0]
	pk.split = (header & split_flag) != 0
	reliability := (header & 0xe0) >> 5
	if reliability > u8(int(Reliability.reliable_sequenced)) {
		return error('unknown packet reliability')
	}
	pk.reliability = unsafe { Reliability(reliability) }
	n := int(binary.big_endian_u16_at(b, 1) >> 3)
	if n == 0 {
		return error('invalid packet length: cannot be 0')
	}
	mut offset := 3
	if pk.reliability.reliable() {
		if b.len - offset < 3 {
			return error('unexpected eof reading packet message index')
		}
		pk.message_index = safe_load_uint24(b[offset..])!
		offset += 3
	}
	if pk.reliability.sequenced() {
		if b.len - offset < 3 {
			return error('unexpected eof reading packet sequence index')
		}
		pk.sequence_index = safe_load_uint24(b[offset..])!
		offset += 3
	}
	if pk.reliability.sequenced_or_ordered() {
		if b.len - offset < 4 {
			return error('unexpected eof reading packet order index')
		}
		pk.order_index = safe_load_uint24(b[offset..])!
		offset += 4
	}
	if pk.split {
		if b.len - offset < 10 {
			return error('unexpected eof reading split packet header')
		}
		pk.split_count = binary.big_endian_u32_at(b, offset)
		pk.split_id = binary.big_endian_u16_at(b, offset + 4)
		pk.split_index = binary.big_endian_u32_at(b, offset + 6)
		offset += 10
	}
	if b.len - offset < n {
		return error('unexpected eof reading packet content')
	}
	pk.content = b[offset..offset + n].clone()
	return offset + n
}

const packet_additional_size = 1 + 3 + 1 + 2 + 3 + 3 + 1
const split_additional_size = 4 + 2 + 4

fn split_packet_content(data []u8, mtu u16) [][]u8 {
	if data.len == 0 {
		return [][]u8{}
	}
	mut max_size := int(mtu) - packet_additional_size
	if data.len > max_size {
		max_size -= split_additional_size
	}
	mut fragments := [][]u8{}
	mut offset := 0
	for offset < data.len {
		end := if offset + max_size > data.len { data.len } else { offset + max_size }
		fragments << data[offset..end].clone()
		offset = end
	}
	return fragments
}
