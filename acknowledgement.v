module raknet

import encoding.binary

const packet_range = u8(0)
const packet_single = u8(1)
const max_acknowledgement_packets = 8192

struct Acknowledgement {
mut:
	packets []Uint24
}

fn (mut ack Acknowledgement) write(mut out []u8, mtu u16) int {
	len_offset := out.len
	out << [u8(0), 0]
	if ack.packets.len == 0 {
		return 0
	}
	mut packets := ack.packets.clone()
	packets.sort(a < b)
	mut first := Uint24(0)
	mut last := Uint24(0)
	mut records := u16(0)
	mut n := 0
	for index, pk in packets {
		if out.len >= int(mtu) - 7 {
			break
		}
		n++
		if index == 0 {
			first = pk
			last = pk
			continue
		}
		if pk == Uint24(u32(last) + 1) {
			last = pk
			continue
		}
		records = write_ack_record(mut out, first, last, records)
		first = pk
		last = pk
	}
	records = write_ack_record(mut out, first, last, records)
	binary.big_endian_put_u16_at(mut out, records, len_offset)
	return n
}

fn write_ack_record(mut out []u8, first Uint24, last Uint24, records u16) u16 {
	if first == last {
		out << packet_single
		write_uint24(mut out, first)
	} else {
		out << packet_range
		write_uint24(mut out, first)
		write_uint24(mut out, last)
	}
	return records + 1
}

fn (mut ack Acknowledgement) read(b []u8) ! {
	if b.len < 2 {
		return error('unexpected eof reading acknowledgement')
	}
	mut offset := 2
	record_count := int(binary.big_endian_u16_at(b, 0))
	for _ in 0 .. record_count {
		if b.len - offset < 4 {
			return error('unexpected eof reading acknowledgement record')
		}
		match b[offset] {
			packet_range {
				if b.len - offset < 7 {
					return error('unexpected eof reading acknowledgement range')
				}
				start := safe_load_uint24(b[offset + 1..])!
				end := safe_load_uint24(b[offset + 4..])!
				if end < start {
					return error('invalid acknowledgement range: end before start')
				}
				count := int(u32(end) - u32(start) + 1)
				if ack.packets.len + count > max_acknowledgement_packets {
					return error('maximum amount of packets in acknowledgement exceeded')
				}
				for i := u32(start); i <= u32(end); i++ {
					ack.packets << Uint24(i)
				}
				offset += 7
			}
			packet_single {
				if ack.packets.len + 1 > max_acknowledgement_packets {
					return error('maximum amount of packets in acknowledgement exceeded')
				}
				ack.packets << safe_load_uint24(b[offset + 1..])!
				offset += 4
			}
			else {
				return error('unknown acknowledgement record type')
			}
		}
	}
}
