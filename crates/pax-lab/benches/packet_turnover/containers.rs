//! Isolate ladder container costs with equivalent ordered outputs. This is not a
//! replacement for full book validation; decode/application remain separate rows.
use super::report;
use std::{collections::BTreeMap, hint::black_box, time::Instant};
pub fn compare(iterations: usize) {
    for depth in [20u32, 200, 2000] {
        for position in ["best", "middle", "tail"] {
            let initial: Vec<_> = (0..depth).map(|i| (100_000 + i * 2, 1000u64)).collect();
            let price = match position {
                "best" => 99_999,
                "middle" => 100_000 + depth - 1,
                _ => 100_000 + depth * 2 + 1,
            };
            let mut vector = initial.clone();
            vector.reserve(1);
            let mut map: BTreeMap<_, _> = initial.iter().copied().collect();
            for mode in ["vector_insert_remove", "ordered_map_insert_remove"] {
                let mut samples = Vec::with_capacity(iterations);
                for round in 0..iterations + 100 {
                    let start = Instant::now();
                    if mode == "vector_insert_remove" {
                        let index = vector.partition_point(|(p, _)| *p < black_box(price));
                        vector.insert(index, (price, black_box(123)));
                        black_box(&vector);
                        let index = vector.partition_point(|(p, _)| *p < black_box(price));
                        black_box(vector.remove(index));
                    } else {
                        map.insert(black_box(price), black_box(123));
                        black_box(&map);
                        black_box(map.remove(&black_box(price)));
                    }
                    let elapsed = start.elapsed();
                    if round >= 100 {
                        samples.push(elapsed);
                    }
                }
                assert_eq!(vector, initial);
                assert_eq!(
                    map.iter().map(|(&p, &q)| (p, q)).collect::<Vec<_>>(),
                    initial
                );
                let identity = serde_json::to_vec(&initial).unwrap();
                report(
                    mode,
                    &format!("container-only-depth{depth}-{position}"),
                    &identity,
                    &mut samples,
                    blake3::hash(&identity).to_hex().as_ref(),
                );
            }
        }
    }
}
