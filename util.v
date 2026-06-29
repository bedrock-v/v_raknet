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
		if end + 1 >= s.len || s[end + 1] != `:` {
			return error('missing port in address ${s}')
		}
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
	if octets.len == 4 {
		return message.AddrPort{
			ip:   [u8(strconv.atoi(octets[0])!), u8(strconv.atoi(octets[1])!),
				u8(strconv.atoi(octets[2])!), u8(strconv.atoi(octets[3])!)]!
			port: u16(strconv.atoi(port_text)!)
		}
	}
	ip6 := parse_ipv6_bytes(host)!
	return message.AddrPort{
		ip6:  ip6
		port: u16(strconv.atoi(port_text)!)
		is6:  true
	}
}

fn normalise_addr_string(s string) string {
	return s.trim_space()
}

fn parse_ipv6_bytes(s string) ![16]u8 {
	if s == '' {
		return error('parse ipv6: empty address')
	}
	double_idx := s.index('::') or { -1 }
	if s.count('::') > 1 {
		return error('parse ipv6: multiple "::" runs')
	}
	head, tail := if double_idx >= 0 { s[..double_idx], s[double_idx + 2..] } else { s, '' }
	mut head_groups := if head == '' { []string{} } else { head.split(':') }
	mut tail_groups := if tail == '' { []string{} } else { tail.split(':') }
	mut v4_bytes := []u8{}
	tail_owns_last := double_idx >= 0
	last_group := if tail_owns_last && tail_groups.len > 0 {
		tail_groups[tail_groups.len - 1]
	} else if !tail_owns_last && head_groups.len > 0 {
		head_groups[head_groups.len - 1]
	} else {
		''
	}
	if last_group.contains('.') {
		v4_bytes = parse_ipv4_bytes(last_group)!
		if tail_owns_last {
			tail_groups = tail_groups[..tail_groups.len - 1].clone()
		} else {
			head_groups = head_groups[..head_groups.len - 1].clone()
		}
	}
	mut head_words := []u16{}
	for group in head_groups {
		head_words << parse_ipv6_hex_group(group)!
	}
	mut tail_words := []u16{}
	for group in tail_groups {
		tail_words << parse_ipv6_hex_group(group)!
	}
	v4_word_len := if v4_bytes.len == 4 { 2 } else { 0 }
	total := head_words.len + tail_words.len + v4_word_len
	if double_idx < 0 {
		if total != 8 {
			return error('parse ipv6: expected 8 groups, got ${total}')
		}
	} else if total >= 8 {
		return error('parse ipv6: "::" address already has ${total} groups')
	}
	mut words := []u16{cap: 8}
	words << head_words
	if double_idx >= 0 {
		for _ in 0 .. (8 - total) {
			words << u16(0)
		}
	}
	words << tail_words
	if v4_bytes.len == 4 {
		words << (u16(v4_bytes[0]) << 8) | u16(v4_bytes[1])
		words << (u16(v4_bytes[2]) << 8) | u16(v4_bytes[3])
	}
	if words.len != 8 {
		return error('parse ipv6: expanded to ${words.len} groups')
	}
	mut out := [16]u8{}
	for i, word in words {
		out[i * 2] = u8(word >> 8)
		out[i * 2 + 1] = u8(word & 0xff)
	}
	return out
}

fn parse_ipv6_hex_group(s string) !u16 {
	if s == '' || s.len > 4 {
		return error('parse ipv6: invalid group "${s}"')
	}
	mut value := u32(0)
	for c in s {
		digit := ipv6_hex_digit(c) or { return error('parse ipv6: invalid hex group "${s}"') }
		value = (value << 4) | u32(digit)
	}
	return u16(value)
}

fn ipv6_hex_digit(c u8) ?u8 {
	if c >= `0` && c <= `9` {
		return u8(c - `0`)
	}
	if c >= `a` && c <= `f` {
		return u8(c - `a` + 10)
	}
	if c >= `A` && c <= `F` {
		return u8(c - `A` + 10)
	}
	return none
}

fn parse_ipv4_bytes(s string) ![]u8 {
	parts := s.split('.')
	if parts.len != 4 {
		return error('invalid ipv4 address ${s}')
	}
	mut out := []u8{cap: 4}
	for part in parts {
		if part.len == 0 {
			return error('invalid ipv4 address ${s}')
		}
		value := strconv.atoi(part)!
		if value < 0 || value > 255 {
			return error('invalid ipv4 address ${s}')
		}
		out << u8(value)
	}
	return out
}
