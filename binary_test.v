module raknet

fn test_uint24() {
	mut value := Uint24(123456)
	assert value.inc() == Uint24(123456)
	assert value == Uint24(123457)

	mut out := []u8{}
	write_uint24(mut out, Uint24(123456))
	assert out == [u8(0x40), 0xe2, 0x01]
	assert load_uint24(out) == Uint24(123456)
}
