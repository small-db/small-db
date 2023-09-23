use crate::io::{Decodeable, Encodeable, SmallWriter};

#[derive(PartialEq, Copy, Clone, Eq, Hash, Debug)]
pub enum PageCategory {
    RootPointer,
    Internal,
    Leaf,
    Header,
}

const ROOT_POINTER: [u8; 4] = [0, 0, 0, 0];
const INTERNAL: [u8; 4] = [0, 0, 0, 1];
const LEAF: [u8; 4] = [0, 0, 0, 2];
const HEADER: [u8; 4] = [0, 0, 0, 3];

impl Decodeable for PageCategory {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
        let mut buffer = [0; 4];
        reader.read_exact(&mut buffer).unwrap();
        match buffer {
            ROOT_POINTER => PageCategory::RootPointer,
            INTERNAL => PageCategory::Internal,
            LEAF => PageCategory::Leaf,
            HEADER => PageCategory::Header,
            _ => panic!("invalid page category: {:?}", buffer),
        }
    }
}

impl Encodeable for PageCategory {
    fn encode(&self, writer: &mut SmallWriter) {
        match self {
            PageCategory::RootPointer => writer.write_bytes(&ROOT_POINTER),
            PageCategory::Internal => writer.write_bytes(&INTERNAL),
            PageCategory::Leaf => writer.write_bytes(&LEAF),
            PageCategory::Header => writer.write_bytes(&HEADER),
        }
    }
}
