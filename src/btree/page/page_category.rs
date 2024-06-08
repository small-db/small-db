use crate::io::{Decodeable, Encodeable, Serializeable, SmallWriter};

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

impl Serializeable for PageCategory {
    type Reference = ();

    fn encode(&self, writer: &mut SmallWriter, _: &Self::Reference) {
        match self {
            PageCategory::RootPointer => writer.write_bytes(&ROOT_POINTER),
            PageCategory::Internal => writer.write_bytes(&INTERNAL),
            PageCategory::Leaf => writer.write_bytes(&LEAF),
            PageCategory::Header => writer.write_bytes(&HEADER),
        }
    }

    fn decode<R: std::io::Read>(reader: &mut R, _: &Self::Reference) -> Self {
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
