from uuid import UUID
import os
import time


## Code taken from open pull request on uuid module in standard library


_last_timestamp_v7 = None
_last_counter_v7 = None

def uuid7():
    """Generate a UUID from a Unix timestamp in milliseconds and random bits.
    UUIDv7 objects feature monotonicity within a millisecond.
    """
    # --- 48 ---   -- 4 --   --- 12 ---   -- 2 --   --- 30 ---   - 32 -
    # unix_ts_ms | version | counter_hi | variant | counter_lo | random
    #
    # 'counter = counter_hi | counter_lo' is a 42-bit counter constructed
    # with Method 1 of RFC 9562, §6.2, and its MSB is set to 0.
    #
    # 'random' is a 32-bit random value regenerated for every new UUID.
    #
    # If multiple UUIDs are generated within the same millisecond, the LSB
    # of 'counter' is incremented by 1. When overflowing, the timestamp is
    # advanced and the counter is reset to a random 42-bit integer with MSB
    # set to 0.

    def get_counter_and_tail():
        rand = int.from_bytes(os.urandom(10), byteorder='big')
        # 42-bit counter with MSB set to 0
        counter = (rand >> 32) & 0x1ffffffffff
        # 32-bit random data
        tail = rand & 0xffffffff
        return counter, tail

    global _last_timestamp_v7
    global _last_counter_v7

    import time
    nanoseconds = time.time_ns()
    timestamp_ms, _ = divmod(nanoseconds, 1_000_000)

    if _last_timestamp_v7 is None or timestamp_ms > _last_timestamp_v7:
        counter, tail = get_counter_and_tail()
    else:
        if timestamp_ms < _last_timestamp_v7:
            timestamp_ms = _last_timestamp_v7 + 1
        # advance the counter
        counter = _last_counter_v7 + 1
        if counter > 0x3ffffffffff:
            timestamp_ms += 1  # advance the timestamp
            counter, tail = get_counter_and_tail()
        else:
            tail = int.from_bytes(os.urandom(4), byteorder='big')

    _last_timestamp_v7 = timestamp_ms
    _last_counter_v7 = counter

    int_uuid_7 = (timestamp_ms & 0xffffffffffff) << 80
    int_uuid_7 |= ((counter >> 30) & 0xfff) << 64
    int_uuid_7 |= (counter & 0x3fffffff) << 32
    int_uuid_7 |= tail & 0xffffffff
    return UUID(int=int_uuid_7)