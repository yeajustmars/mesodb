// src/lib.rs
pub mod datom;
pub mod memtable;
pub mod schema;
pub mod types;
pub mod wal;
// pub mod error; // To be implemented next
// pub mod memtable; // To be implemented next

#[cfg(test)]
mod tests {
    //use super::*;

    //#[test]
    //fn it_works() {
    //    let result = add(2, 2);
    //    assert_eq!(result, 4);
    //}
}
