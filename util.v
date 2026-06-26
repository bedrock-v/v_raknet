module raknet

import strconv
import time
import message

const start_time = time.now()

fn timestamp() i64 {
	return (time.now() - start_time).milliseconds()
}

fn addr_port_from_string(s string) !message.AddrPort {
	mut host := s
	mut port_text := ''
	if s.starts_with('[') {
		end := s.index(']') or { return error('invalid ipv6 address ${s}') }
		host = s[1..end]
		port_text = s[end + 2..]
	} else {
		parts := s.split(':')
		if parts.len < 2 {
			return error('missing port in address ${s}')
		}
		port_text = parts[parts.len - 1]
		host = parts[..parts.len - 1].join(':')
	}
	octets := host.split('.')
	if octets.len != 4 {
		return error('ipv6 addresses are not supported')
	}
	return message.AddrPort{
		ip:   [u8(strconv.atoi(octets[0])!), u8(strconv.atoi(octets[1])!),
			u8(strconv.atoi(octets[2])!), u8(strconv.atoi(octets[3])!)]!
		port: u16(strconv.atoi(port_text)!)
	}
}

fn normalise_addr_string(s string) string {
	return s.trim_space()
}
