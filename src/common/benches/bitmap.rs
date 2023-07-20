// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bitvec::bitvec;
use bitvec::vec::BitVec;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use itertools::Itertools;
use rand::{Rng, SeedableRng};
use risingwave_common::buffer::Bitmap;

fn rng() -> impl rand::Rng {
    rand::rngs::SmallRng::seed_from_u64(0x114514)
}

fn bench_bitmap(c: &mut Criterion) {
    const CHUNK_SIZE: usize = 1024;
    let x = Bitmap::zeros(CHUNK_SIZE);
    let y = Bitmap::ones(CHUNK_SIZE);
    let i = 0x123;
    c.bench_function("bitmap_zeros", |b| b.iter(|| Bitmap::zeros(CHUNK_SIZE)));
    c.bench_function("bitmap_ones", |b| b.iter(|| Bitmap::ones(CHUNK_SIZE)));
    c.bench_function("bitmap_get", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(x.is_set(i));
            }
        })
    });
    c.bench_function("bitmap_get_1000", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(x.is_set(i));
            }
        })
    });
    c.bench_function("bitmap_get_1000_000", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                black_box(x.is_set(i));
            }
        })
    });
    c.bench_function("bitmap_and", |b| b.iter(|| &x & &y));
    c.bench_function("bitmap_or", |b| b.iter(|| &x | &y));
    c.bench_function("bitmap_not", |b| b.iter(|| !&x));
}

fn bench_bitvec(c: &mut Criterion) {
    const CHUNK_SIZE: usize = 1024;
    let x = bitvec![0; CHUNK_SIZE];
    let y = bitvec![1; CHUNK_SIZE];
    let i = 0x123;
    c.bench_function("bitvec_zeros", |b| b.iter(|| bitvec![0; CHUNK_SIZE]));
    c.bench_function("bitvec_ones", |b| b.iter(|| bitvec![1; CHUNK_SIZE]));
    c.bench_function("bitvec_get", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(x[i]);
            }
        })
    });
    c.bench_function("bitvec_get_1000", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(x[i]);
            }
        })
    });
    c.bench_function("bitvec_get_1000_000", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                black_box(x[i]);
            }
        })
    });
    c.bench_function("bitvec_and", |b| b.iter(|| x.clone() & y.clone()));
    c.bench_function("bitvec_or", |b| b.iter(|| x.clone() | y.clone()));
    c.bench_function("bitvec_not", |b| b.iter(|| !(x.clone())));
}

/// Bench ~10 million records
fn bench_bitmap_iter(c: &mut Criterion) {
    const CHUNK_SIZE: usize = 1024;
    const N_CHUNKS: usize = 10_000;

    fn bench_bitmap_iter_inner(bench_id: &str, bitmap: Bitmap, c: &mut Criterion) {
        let bitmaps = vec![bitmap; N_CHUNKS];
        // let make_iters = || make_iterators(&bitmaps);
        c.bench_function(&format!("{bench_id}_iter"), |b| {
            b.iter_batched(
                || bitmaps.iter().map(|b| b.iter()).collect_vec(),
                |iters| {
                    for iter in iters {
                        for bit_flag in iter {
                            black_box(bit_flag);
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });
        c.bench_function(&format!("{bench_id}_iter_ones"), |b| {
            b.iter_batched(
                || bitmaps.iter().map(|b| b.iter_ones()).collect_vec(),
                |iters| {
                    for iter in iters {
                        for index in iter {
                            black_box(index);
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    let zeros = Bitmap::zeros(CHUNK_SIZE);
    bench_bitmap_iter_inner("bitmap_zeros", zeros, c);
    let ones = Bitmap::ones(CHUNK_SIZE);
    bench_bitmap_iter_inner("bitmap_ones", ones, c);

    let mut rng = rng();
    let random = std::iter::repeat_with(|| rng.gen::<bool>())
        .take(CHUNK_SIZE)
        .collect();
    bench_bitmap_iter_inner("bitmap_random", random, c);
}

/// Bench ~10 million records
fn bench_bitvec_iter(c: &mut Criterion) {
    const CHUNK_SIZE: usize = 1024;
    const N_CHUNKS: usize = 10_000;

    fn bench_bitmap_iter_inner(bench_id: &str, bitmap: BitVec, c: &mut Criterion) {
        let bitmaps = vec![bitmap; N_CHUNKS];
        // let make_iters = || make_iterators(&bitmaps);
        c.bench_function(&format!("{bench_id}_iter"), |b| {
            b.iter_batched(
                || bitmaps.iter().map(|b| b.iter()).collect_vec(),
                |iters| {
                    for iter in iters {
                        for bit_flag in iter {
                            black_box(bit_flag);
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });
        c.bench_function(&format!("{bench_id}_iter_ones"), |b| {
            b.iter_batched(
                || bitmaps.iter().map(|b| b.iter_ones()).collect_vec(),
                |iters| {
                    for iter in iters {
                        for index in iter {
                            black_box(index);
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    let zeros = bitvec![0; CHUNK_SIZE];
    bench_bitmap_iter_inner("bitvec_zeros", zeros, c);
    let ones = bitvec![1; CHUNK_SIZE];
    bench_bitmap_iter_inner("bitvec_ones", ones, c);

    let mut rng = rng();
    let random = std::iter::repeat_with(|| rng.gen::<bool>())
        .take(CHUNK_SIZE)
        .collect();
    bench_bitmap_iter_inner("bitvec_random", random, c);
}

criterion_group!(
    benches,
    bench_bitmap,
    bench_bitmap_iter,
    bench_bitvec,
    bench_bitvec_iter
);
criterion_main!(benches);
