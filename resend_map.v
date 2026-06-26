module raknet

import time

struct ResendRecord {
	pk        Packet
	timestamp time.Time
}

struct DelayRecord {
	at    time.Time
	delay time.Duration
}

struct ResendMap {
mut:
	unacknowledged map[Uint24]ResendRecord
	delays         []DelayRecord
}

fn new_resend_map() ResendMap {
	return ResendMap{
		unacknowledged: map[Uint24]ResendRecord{}
	}
}

fn (mut m ResendMap) add(index Uint24, pk Packet) {
	m.add_at(index, pk, time.now())
}

fn (mut m ResendMap) add_at(index Uint24, pk Packet, at time.Time) {
	m.unacknowledged[index] = ResendRecord{
		pk:        pk.clone()
		timestamp: at
	}
}

fn (mut m ResendMap) acknowledge(index Uint24) (Packet, bool) {
	return m.acknowledge_at(index, time.now())
}

fn (mut m ResendMap) acknowledge_at(index Uint24, now time.Time) (Packet, bool) {
	return m.remove_at(index, now, 1)
}

fn (mut m ResendMap) retransmit(index Uint24) (Packet, bool) {
	return m.retransmit_at(index, time.now())
}

fn (mut m ResendMap) retransmit_at(index Uint24, now time.Time) (Packet, bool) {
	return m.remove_at(index, now, 2)
}

fn (mut m ResendMap) remove(index Uint24) (Packet, bool) {
	return m.remove_at(index, time.now(), 1)
}

fn (mut m ResendMap) remove_at(index Uint24, now time.Time, mul int) (Packet, bool) {
	record := m.unacknowledged[index] or { return Packet{}, false }
	m.unacknowledged.delete(index)
	m.delays << DelayRecord{
		at:    now
		delay: (now - record.timestamp) * mul
	}
	return record.pk.clone(), true
}

fn (m ResendMap) keys() []Uint24 {
	return m.unacknowledged.keys()
}

fn (m ResendMap) due(now time.Time, delay time.Duration) []Uint24 {
	mut out := []Uint24{}
	for index, record in m.unacknowledged {
		if now - record.timestamp > delay {
			out << index
		}
	}
	return out
}

fn (m ResendMap) len() int {
	return m.unacknowledged.len
}

fn (mut m ResendMap) rtt(now time.Time) time.Duration {
	cutoff := 5 * time.second
	mut kept := []DelayRecord{}
	for record in m.delays {
		if now - record.at <= cutoff {
			kept << record
		}
	}
	m.delays = kept
	if m.delays.len == 0 {
		return 50 * time.millisecond
	}
	mut total := time.Duration(0)
	for record in m.delays {
		total += record.delay
	}
	return total / m.delays.len
}
