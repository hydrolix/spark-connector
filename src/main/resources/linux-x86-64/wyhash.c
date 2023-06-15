// Copyright 2020 王一 Wang Yi <godspeed_china@yeah.net>
//   This is free and unencumbered software released into the public domain.
//   http://unlicense.org/ See github.com/wangyi-fudan/wyhash/ LICENSE

#include <stdint.h>
#include <stdbool.h>

#define H_ALWAYS_INLINE inline __attribute__((always_inline))
#define h_likely(condition) __builtin_expect((bool)(condition), 1)
#define h_unlikely(condition) __builtin_expect((bool)(condition), 0)

#define WY0 0xa0761d6478bd642full
#define WY1 0xe7037ed1a0b428dbull
#define WY2 0x8ebc6af09c88c6e3ull
#define WY3 0x589965cc75374cc3ull
#define WY4 0x1d8e4e27c47d124full

static H_ALWAYS_INLINE void
_wymum(uint64_t *A, uint64_t *B)
{
    __uint128_t r = *A;
    r *= *B;

    *A = (uint64_t)r;
    *B = (uint64_t)(r >> 64);
}

static H_ALWAYS_INLINE uint64_t
_wymix(uint64_t A, uint64_t B)
{
    _wymum(&A, &B);
    return A ^ B;
}

static H_ALWAYS_INLINE uint64_t
_wyr8(const uint8_t *p)
{
    uint64_t v;
    __builtin_memcpy_inline(&v, p, 8);
    return v;
}

static H_ALWAYS_INLINE uint64_t
_wyfinish16(const uint8_t *p, uint64_t len, uint64_t seed, uint64_t i)
{
    uint64_t a, b;
    if (h_likely(i <= 8)) {
        if (h_likely(i >= 4)) {
            a = b = 0;
            __builtin_memcpy_inline(&a, p, 4);
            __builtin_memcpy_inline(&b, p + i - 4, 4);
        } else if (h_likely(i)) {
            a = ((uint64_t)(p[0]) << 16) | ((uint64_t)(p[i >> 1]) << 8) |
                p[i - 1];
            b = 0;
        } else
            a = b = 0;
    } else {
        __builtin_memcpy_inline(&a, p, 8);
        __builtin_memcpy_inline(&b, p + i - 8, 8);
    }

    return _wymix(WY1 ^ len, _wymix(a ^ WY1, b ^ seed));
}

static H_ALWAYS_INLINE uint64_t
_wyfinish(const uint8_t *p, uint64_t len, uint64_t seed, uint64_t i)
{
    if (h_likely(i <= 16))
        return _wyfinish16(p, len, seed, i);
    return _wyfinish(p + 16, len, _wymix(_wyr8(p) ^ WY1, _wyr8(p + 8) ^ seed), i - 16);
}

uint64_t wyhash(const void *key, uint64_t len, uint64_t seed) {
    const uint8_t *p = (const uint8_t*)key;
    uint64_t i = len;
    seed ^= WY0;
    if (h_unlikely(i > 64)) {
        uint64_t see1 = seed;
        do {
            seed = _wymix(_wyr8(p) ^ WY1, _wyr8(p + 8) ^ seed) ^
                   _wymix(_wyr8(p + 16) ^ WY2, _wyr8(p + 24) ^ seed);
            see1 = _wymix(_wyr8(p + 32) ^ WY3, _wyr8(p + 40) ^ see1) ^
                   _wymix(_wyr8(p + 48) ^ WY4, _wyr8(p + 56) ^ see1);
            p += 64;
            i -= 64;
        } while (i > 64);
        seed ^= see1;
    }
    return _wyfinish(p, len, seed, i);
}

static H_ALWAYS_INLINE uint64_t
wyhash64(uint64_t A, uint64_t B)
{
    __uint128_t r = A ^ WY0;
    r *= B ^ WY1;

    A = (uint64_t)(r) ^ WY0;
    B = (uint64_t)(r >> 64) ^ WY1;

    r = A;
    r *= B;

    A = (uint64_t)(r);
    B = (uint64_t)(r >> 64);

    return A ^ B;
}
