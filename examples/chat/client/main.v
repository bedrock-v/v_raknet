module main

import os
import raknet

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

fn read_server(mut conn raknet.Conn, done chan bool) {
	for {
		packet := conn.read_packet() or {
			done <- true
			return
		}
		println(packet.bytestr())
	}
}

fn main() {
	args := cli_args()
	address := if args.len > 0 { args[0] } else { '127.0.0.1:19132' }
	name := if args.len > 1 { args[1] } else { 'guest' }

	mut conn := raknet.dial(address) or {
		eprintln('dial failed: ${err.msg()}')
		return
	}
	defer {
		conn.close() or {}
	}

	done := chan bool{cap: 1}
	spawn read_server(mut conn, done)
	conn.write(name.bytes()) or {
		eprintln('write name failed: ${err.msg()}')
		return
	}

	println('connected to ${address} as ${name}')
	println('type /quit to leave')
	for {
		select {
			_ := <-done {
				eprintln('disconnected')
				return
			}
			else {}
		}
		line := os.input('')
		text := line.trim_space()
		if text == '' {
			continue
		}
		conn.write(text.bytes()) or {
			eprintln('write failed: ${err.msg()}')
			return
		}
		if text == '/quit' {
			return
		}
	}
}
