module raknet

import encoding.binary
import hash.crc32
import net
import message

fn cookie_for_addr(addr net.Addr, salt u64) u32 {
	ap := addr_port_from_string(addr.str()) or { return 0 }
	return cookie_for_addr_port(ap, salt)
}

fn cookie_for_addr_port(ap message.AddrPort, salt u64) u32 {
	mut b := []u8{len: 10}
	binary.little_endian_put_u64_at(mut b, salt, 0)
	binary.little_endian_put_u16_at(mut b, ap.port, 8)
	if ap.is6 {
		for part in ap.ip6 {
			b << part
		}
	} else {
		for part in ap.ip {
			b << part
		}
	}
	return crc32.sum(b)
}
