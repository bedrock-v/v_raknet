module raknet

pub type Uint24 = u32

// In V, a mutable receiver for an alias type behaves pointer-like here,
// so copy the underlying value explicitly before incrementing.
pub fn (mut u Uint24) inc() Uint24 {
	old := Uint24(*u)
	u = Uint24(u32(old) + 1)
	return old
}

pub fn load_uint24(b []u8) Uint24 {
	return Uint24(u32(b[0]) | (u32(b[1]) << 8) | (u32(b[2]) << 16))
}

pub fn safe_load_uint24(b []u8) !Uint24 {
	if b.len < 3 {
		return error('unexpected eof reading uint24')
	}
	return load_uint24(b)
}

pub fn write_uint24(mut b []u8, v Uint24) {
	b << u8(v)
	b << u8(u32(v) >> 8)
	b << u8(u32(v) >> 16)
}
