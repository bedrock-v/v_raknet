module raknet

struct PacketQueue {
mut:
	lowest  Uint24
	highest Uint24
	queue   map[Uint24][]u8
}

fn new_packet_queue() PacketQueue {
	return PacketQueue{
		queue: map[Uint24][]u8{}
	}
}

fn (mut q PacketQueue) put(index Uint24, packet []u8) bool {
	if index < q.lowest {
		return false
	}
	if index in q.queue {
		return false
	}
	if index >= q.highest {
		q.highest = Uint24(u32(index) + 1)
	}
	q.queue[index] = packet.clone()
	return true
}

fn (mut q PacketQueue) fetch() [][]u8 {
	mut packets := [][]u8{}
	mut index := q.lowest
	for index < q.highest {
		packet := q.queue[index] or { break }
		q.queue.delete(index)
		packets << packet
		index = Uint24(u32(index) + 1)
	}
	q.lowest = index
	return packets
}

fn (q PacketQueue) window_size() Uint24 {
	return Uint24(u32(q.highest) - u32(q.lowest))
}
