//! Simple implementation of "S3-FIFO" from "FIFO Queues are ALL You Need for Cache Eviction" by
//! Juncheng Yang, et al: https://jasony.me/publication/sosp23-s3fifo.pdf

use std::collections::VecDeque;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::SeqCst;

// The paper uses two bits to count accesses, for a max of 3. We use 8 bit atomics, but will limit
// the count to the same value, to prevent wrap-arounds causing problems.
const MAX_FREQ: u8 = 3;

struct Entry<K, V> {
    key: K,
    value: V,
    freq: AtomicU8,
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            freq: AtomicU8::new(0),
        }
    }
}

pub struct S3Fifo<K: PartialEq, V> {
    small: VecDeque<Entry<K, V>>,
    main: VecDeque<Entry<K, V>>,
    ghost: VecDeque<K>,
    small_size: usize,
    small_min_size: usize,
    small_max_size: usize,
    main_size: usize,
    insert_count: usize, // 跟踪插入次数
    small_operated: bool, // 标记自上次调整以来是否有操作发生在small队列上
}

impl<K: PartialEq, V> S3Fifo<K, V> {
    pub fn new(small: usize,small_min:usize,small_max:usize, main: usize,insert_count:usize,small_operated:bool) -> Self {
        Self {
            small: VecDeque::with_capacity(small),
            main: VecDeque::with_capacity(main),
            ghost: VecDeque::with_capacity(main),
            small_size: small,
            small_min_size:small_min,
            small_max_size:small_max,
            main_size: main,
            insert_count: insert_count, // 跟踪插入次数
            small_operated: small_operated, // 标记自上次调整以来是否有操作发生在small队列上
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let operated_on_small = self.small.iter().any(|entry| &entry.key == &key);
        //self.adjust_small_size();
        if operated_on_small {
            self.small_operated = true;
        }
        // This could be implemented using lock-free queues to not require &mut self, but that is
        // left as an exercise to the reader.
        if self.ghost.contains(&key) {
            if self.main.len() >= self.main_size {
                self.evict_main();
            }
            self.main.push_front(Entry::new(key, value));
        } else {
            if self.small.len() >= self.small_size {
                self.evict_small();
            }
            self.small.push_front(Entry::new(key, value));
        }
        self.insert_count += 1;
        // 每三次插入操作后，检查是否需要调整队列大小
        if self.insert_count >= 3 {
            self.adjust_small_size();
            self.insert_count = 0; // 重置插入计数
            self.small_operated = false; // 重置操作标记
    }
}

    pub fn read(&self, key: &K) -> Option<&V> {
        if let Some(entry) = self.small.iter()
            .chain(self.main.iter())
            .find(|e| &e.key == key)
        {
            if entry.freq.fetch_add(1, SeqCst) + 1 > MAX_FREQ {
                // Clamp it.
                entry.freq.store(MAX_FREQ, SeqCst);
            }
            Some(&entry.value)
        } else {
            None
        }

    }

    fn evict_main(&mut self) {
        while let Some(tail) = self.main.pop_back() {
            let n = tail.freq.load(SeqCst);
            if n > 0 {
                tail.freq.store(n - 1, SeqCst);
                self.main.push_front(tail);
            } else {
                break;
            }
        }
    }

    fn evict_small(&mut self) {
        if let Some(tail) = self.small.pop_back() {
            if tail.freq.load(SeqCst) > 1 {
                if self.main.len() >= self.main_size {
                    self.evict_main();
                }
                self.main.push_front(tail);
            } else {
                if self.ghost.len() >= self.main_size {
                    self.ghost.pop_back();
                }
                self.ghost.push_front(tail.key);
            }
        }
    }

    fn adjust_small_size(&mut self) {
        if self.should_increase_small() {
            eprintln!("increase_small");
            self.small_size = std::cmp::min(self.small_size + 1, self.small_max_size);
        } else if self.should_decrease_small() {
            eprintln!("decrease_small");
            self.small_size = std::cmp::max(self.small_size - 1, self.small_min_size);
        }
    }
    // 定义何时增加small队列大小的条件
    fn should_increase_small(&self) -> bool {
        self.small.len() == self.small_size 
    }

    // 定义何时减少small队列大小的条件
    fn should_decrease_small(&self) -> bool {
        self.small_operated && self.small_size > self.small_min_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    #[test]
    fn it_works() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let mut q = S3Fifo::<u32, u32>::new(4,2,8, 20,0,false);

        let mut hit_rate = (0, 0);
        for i in 0 .. 10_000 {
            eprintln!("#{i}");
            let k = rng.gen_range(0..50);
            if rng.gen_bool(0.5) {
                eprintln!("read {k}");
                match q.read(&k) {
                    Some(v) => {
                        eprintln!("hit");
                        assert_eq!(v, &k);
                        hit_rate.0 += 1;
                        hit_rate.1 += 1;
                    }
                    None => {
                        eprintln!("miss");
                        assert!( q.main.iter().chain(q.small.iter()).find(|e| e.key == k).is_none());
                        hit_rate.1 += 1;
                    }
                }
            } else {
                eprintln!("insert {k}");
                q.insert(k, k);
            }
            assert!(q.main.len() <= q.main_size);
            assert!(q.small.len() <= q.small_size);
            assert!(q.ghost.len() <= q.main_size);
        }
        let (n, d) = hit_rate;
        println!("{n}/{d} = {}", (n as f64) / (d as f64));
    }
}
