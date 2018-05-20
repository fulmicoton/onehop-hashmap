extern crate onehop_hashmap;

use std::fs::File;
use std::io::{BufReader, BufRead};


#[inline(always)]
fn update(val: &mut Val) {
    val.0 += 1u32;
}

#[repr(packed)]
#[derive(Copy, Clone)]
struct Val(u32);

fn main() {
    let mut hashmap: onehop_hashmap::HashMap<Val> = onehop_hashmap::HashMap::new();
    let file = File::open("/home/paul/git/onehop-hashmap/text.txt").unwrap();
    let buf = BufReader::new(file);
    for line_res in buf.lines() {
        let line = line_res.unwrap();
        for token in line.split(|c: char| !c.is_alphabetic()) {
            hashmap.update(token.as_bytes(), update, {|| Val(0) });// (token.as_bytes(), 0);
        }
    }
    println!("{}", hashmap.len());
}