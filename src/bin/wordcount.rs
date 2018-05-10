extern crate onehop_hashmap;
//
//use onehop_hashmap::HashMap;

extern crate fnv;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufRead};
use fnv::FnvHashMap;

fn main() {
    let mut hashmap: onehop_hashmap::HashMap<u32> = onehop_hashmap::HashMap::new();
    let file = File::open("/home/paul/git/onehop-hashmap/text.txt").unwrap();
    let buf = BufReader::new(file);
    for line_res in buf.lines() {
        let line = line_res.unwrap();
        for token in line.split(|c: char| !c.is_alphabetic()) {
            hashmap.update(token.as_bytes(), |old_val| {old_val.unwrap_or(0) + 1 });// (token.as_bytes(), 0);
        }
    }
    println!("{}", hashmap.len());
//    println!("{}", hashmap.max_probe_dist());
}


//fn main() {
//    let mut hashmap: FnvHashMap<String, u32> = FnvHashMap::with_capacity_and_hasher(5_000_000, Default::default());
//    let file = File::open("/home/paul/git/onehop-hashmap/text.txt").unwrap();
//    let buf = BufReader::new(file);
//    for line_res in buf.lines() {
//        let line = line_res.unwrap();
//        for token in line.split(|c: char| !c.is_alphabetic()) {
//            {
//                if let Some(count_mut) = hashmap.get_mut(token) {
//                    *count_mut += 1;
//                    continue;
//                }
//            }
//            {
//                hashmap.insert(token.to_string(), 1);
//            }
//        }
//    }
//    println!("{}", hashmap.len());
//}
