module raknet

import time

pub const protocol_version = u8(11)
pub const min_mtu_size = u16(400)
pub const max_mtu_size = u16(1492)
const max_window_size = 2048
const max_split_count = 512
const max_concurrent_splits = 16
const close_drain_timeout = time.second

fn clamp_mtu(mtu u16, min_mtu u16) u16 {
	if mtu == 0 || mtu > max_mtu_size {
		return max_mtu_size
	}
	if mtu < min_mtu {
		return min_mtu
	}
	return mtu
}
