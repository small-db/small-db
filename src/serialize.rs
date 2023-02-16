
// use std::fmt::Debug;

// use crate::io::Condensable;

// pub trait PrimitiveType:
//     Copy + Send + Sync + Default + Debug + 'static
// {
//     fn to_le_bytes(self) -> [u8];
// }

// impl PrimitiveType for u8 {}
// impl PrimitiveType for u16 {}

// pub struct PrimitiveStruct<T: PrimitiveType> {
//     v: T,
// }

// impl<T: PrimitiveType> Condensable for PrimitiveStruct<T> {
//     fn to_bytes(&self) -> Vec<u8> {
//         self.v.to_le_bytes().to_vec()
//     }
// }
