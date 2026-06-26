module raknet

import time

struct DatagramWindow {
mut:
	lowest  Uint24
	highest Uint24
	queue   map[Uint24]time.Time
}

fn new_datagram_window() DatagramWindow {
	return DatagramWindow{
		queue: map[Uint24]time.Time{}
	}
}

fn (mut win DatagramWindow) add(index Uint24) bool {
	return win.add_at(index, time.now())
}

fn (mut win DatagramWindow) add_at(index Uint24, at time.Time) bool {
	if win.seen(index) {
		return false
	}
	if index >= win.highest {
		win.highest = Uint24(u32(index) + 1)
	}
	win.queue[index] = at
	return true
}

fn (win DatagramWindow) seen(index Uint24) bool {
	if index < win.lowest {
		return true
	}
	return index in win.queue
}

fn (mut win DatagramWindow) shift() int {
	mut n := 0
	mut index := win.lowest
	for index < win.highest {
		if index !in win.queue {
			break
		}
		win.queue.delete(index)
		n++
		index = Uint24(u32(index) + 1)
	}
	win.lowest = index
	return n
}

fn (mut win DatagramWindow) missing_all() []Uint24 {
	mut indices := []Uint24{}
	for i := u32(win.lowest); i < u32(win.highest); i++ {
		index := Uint24(i)
		if index !in win.queue {
			indices << index
			win.queue[index] = time.Time{}
		}
	}
	win.shift()
	return indices
}

fn (mut win DatagramWindow) missing(since time.Duration, now time.Time) []Uint24 {
	mut indices := []Uint24{}
	mut missing := false
	for raw := int(win.highest) - 1; raw >= int(win.lowest); raw-- {
		index := Uint24(raw)
		t := win.queue[index] or {
			if missing {
				indices << index
				win.queue[index] = time.Time{}
			}
			continue
		}
		if now - t >= since {
			missing = true
		}
	}
	win.shift()
	return indices
}

fn (win DatagramWindow) size() Uint24 {
	return Uint24(u32(win.highest) - u32(win.lowest))
}
