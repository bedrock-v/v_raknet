module main

import os
import sync
import raknet

@[heap]
struct Room {
mut:
	next_id int
	clients map[int]&raknet.Conn
	names   map[int]string
	mutex   &sync.Mutex = sync.new_mutex()
}

fn new_room() &Room {
	return &Room{
		clients: map[int]&raknet.Conn{}
		names:   map[int]string{}
		mutex:   sync.new_mutex()
	}
}

fn (mut room Room) add(conn &raknet.Conn) int {
	room.mutex.lock()
	id := room.next_id
	room.next_id++
	room.clients[id] = conn
	room.names[id] = 'guest-${id}'
	room.mutex.unlock()
	return id
}

fn (mut room Room) set_name(id int, name string) {
	room.mutex.lock()
	room.names[id] = name
	room.mutex.unlock()
}

fn (mut room Room) remove(id int) string {
	room.mutex.lock()
	name := room.names[id] or { 'guest-${id}' }
	room.clients.delete(id)
	room.names.delete(id)
	room.mutex.unlock()
	return name
}

fn (mut room Room) broadcast(from int, message string) {
	room.mutex.lock()
	mut clients := map[int]&raknet.Conn{}
	for id, conn in room.clients {
		clients[id] = conn
	}
	name := room.names[from] or { 'server' }
	room.mutex.unlock()

	line := if from < 0 { message } else { '${name}: ${message}' }
	data := line.bytes()

	for id, conn in clients {
		if id == from {
			continue
		}

		conn.write(data) or { eprintln('broadcast to ${id} failed: ${err.msg()}') }
	}

	println(line)
}

fn handle_client(mut room Room, conn &raknet.Conn) {
	id := room.add(conn)

	conn.write('name?'.bytes()) or {}

	name_data := conn.read_packet() or {
		room.remove(id)
		return
	}

	name := clean_name(name_data.bytestr(), id)
	room.set_name(id, name)
	room.broadcast(-1, '${name} joined')

	defer {
		left := room.remove(id)
		room.broadcast(-1, '${left} left')
		conn.close() or {}
	}

	for {
		data := conn.read_packet() or { return }
		text := data.bytestr().trim_space()

		if text == '' {
			continue
		}

		if text == '/quit' {
			return
		}

		room.broadcast(id, text)
	}
}

fn clean_name(raw string, id int) string {
	name := raw.trim_space()
	if name == '' {
		return 'guest-${id}'
	}
	if name.len > 24 {
		return name[..24]
	}
	return name
}

fn cli_args() []string {
	mut args := []string{}
	for arg in os.args[1..] {
		if arg == '--' {
			continue
		}
		args << arg
	}
	return args
}

fn main() {
	args := cli_args()
	address := if args.len > 0 { args[0] } else { '127.0.0.1:19132' }
	mut listener := raknet.listen(address) or {
		eprintln('listen failed: ${err.msg()}')
		return
	}
	defer {
		listener.close() or {}
	}
	listener.set_pong_data('raknet chat'.bytes())
	mut room := new_room()
	println('chat server listening on ${listener.addr()}')

	for {
		conn := listener.accept() or {
			eprintln('accept failed: ${err.msg()}')
			break
		}
		spawn handle_client(mut room, conn)
	}
}
