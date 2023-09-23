use crate::io::{Decodeable, Encodeable, SmallWriter};

#[derive(PartialEq, Copy, Clone, Eq, Hash, Debug)]
pub enum PageCategory {
    RootPointer,
    Internal,
    Leaf,
    Header,
}

impl Decodeable for PageCategory {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
        let mut buffer = [0; 4];
        reader.read_exact(&mut buffer).unwrap();
        match buffer {
            [0, 0, 0, 0] => PageCategory::RootPointer,
            [0, 0, 0, 1] => PageCategory::Internal,
            [0, 0, 0, 2] => PageCategory::Leaf,
            [0, 0, 0, 3] => PageCategory::Header,
            _ => panic!("invalid page category: {:?}", buffer),
        }
    }
}

impl Encodeable for PageCategory {
    fn encode(&self, writer: &mut SmallWriter) {
        let v: u8 = match self {
            PageCategory::RootPointer => 0,
            PageCategory::Internal => 1,
            PageCategory::Leaf => 2,
            PageCategory::Header => 3,
        };
        writer.write(&(0 as u8));
        writer.write(&(0 as u8));
        writer.write(&(0 as u8));
        writer.write(&(v));
    }
}
