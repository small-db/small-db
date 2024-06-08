use std::{fmt::Debug, io::Read};

use crate::{
    error::SmallError,
    io::{read_exact, read_into, Decodeable, Encodeable, Serializeable, SmallWriter},
    storage::table_schema::{self, Type},
    TableSchema,
};

#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Bytes(Vec<u8>),
}

impl Cell {
    pub fn new_bool(v: bool) -> Self {
        Cell::Bool(v)
    }

    pub fn get_bool(&self) -> Result<bool, SmallError> {
        match self {
            Cell::Bool(v) => Ok(*v),
            _ => Err(SmallError::new("not bool")),
        }
    }

    pub fn new_int64(v: i64) -> Self {
        Cell::Int64(v)
    }

    pub fn get_int64(&self) -> Result<i64, SmallError> {
        match self {
            Cell::Int64(v) => Ok(*v),
            _ => Err(SmallError::new("not int64")),
        }
    }

    pub fn new_float64(v: f64) -> Self {
        Cell::Float64(v)
    }

    pub fn get_float64(&self) -> Result<f64, SmallError> {
        match self {
            Cell::Float64(v) => Ok(*v),
            _ => Err(SmallError::new("not float64")),
        }
    }

    pub fn new_bytes(v: &[u8], t: &Type) -> Cell {
        match t {
            Type::Bytes(size) => {
                if v.len() > *size as usize {
                    panic!("bytes size too large");
                }

                Cell::Bytes(v.to_vec())
            }
            _ => panic!("not bytes"),
        }
    }

    pub fn get_bytes(&self) -> Result<Vec<u8>, SmallError> {
        match self {
            Cell::Bytes(v) => Ok(v.clone()),
            _ => Err(SmallError::new("not bytes")),
        }
    }

    // pub(crate) fn read_from<R: std::io::Read>(reader: &mut R, t: &Type) -> Self {
    //     match t {
    //         Type::Bool => Cell::Bool(bool::decode_from(reader, &())),
    //         Type::Int64 => Cell::Int64(i64::decode_from(reader)),
    //         Type::Float64 => Cell::Float64(f64::decode_from(reader)),
    //         Type::Bytes(x) => {
    //             // read size
    //             let size: u16 = read_into(reader);

    //             // read payload
    //             let payload = read_exact(reader, *x as usize);

    //             let actual = payload[..size as usize].to_vec();

    //             return Cell::Bytes(actual);
    //         }
    //     }
    // }
}

impl Serializeable for Cell {
    type Reference = Type;

    fn encode(&self, writer: &mut SmallWriter, reference: &Self::Reference) {
        match self {
            Cell::Null => todo!(),
            Cell::Bool(v) => {
                v.encode(writer, &());
            }
            Cell::Int64(v) => {
                v.encode(writer, &());
            }
            Cell::Float64(v) => {
                v.encode(writer, &());
            }
            Cell::Bytes(v) => {
                // write payload size
                let size = v.len() as u16;
                size.encode(writer, &());

                // write payload
                writer.write_bytes(v);

                // padding
                if let Type::Bytes(size) = reference {
                    let remain = *size as usize - v.len();
                    for _ in 0..remain {
                        writer.write(&0u8, &());
                    }
                } else {
                    panic!("type not match, expect bytes, got {:?}", reference);
                }
            }
        }
    }

    fn decode<R: Read>(reader: &mut R, reference: &Self::Reference) -> Self {
        match reference {
            Type::Bool => Cell::Bool(bool::decode(reader, &())),
            Type::Int64 => Cell::Int64(i64::decode(reader, &())),
            Type::Float64 => Cell::Float64(f64::decode(reader, &())),
            Type::Bytes(x) => {
                // read size
                let size = u16::decode(reader, &());

                // read payload
                let payload = read_exact(reader, *x as usize);

                let actual = payload[..size as usize].to_vec();

                return Cell::Bytes(actual);
            }
        }
    }

    fn encode(&self, writer: &mut SmallWriter, reference: &Self::Reference) {
        match self {
            Cell::Null => todo!(),
            Cell::Bool(v) => {
                writer.write_disk_format(v);
            }
            Cell::Int64(v) => {
                writer.write_disk_format(v);
            }
            Cell::Float64(v) => {
                writer.write_disk_format(v);
            }
            Cell::Bytes(v) => {
                // write payload size
                let size = v.len() as u16;
                writer.write_disk_format(&size);

                // write payload
                writer.write_bytes(v);

                // padding
                if let Type::Bytes(size) = reference {
                    let remain = *size as usize - v.len();
                    for _ in 0..remain {
                        writer.write_disk_format(&0u8);
                    }
                } else {
                    panic!("type not match, expect bytes, got {:?}", reference);
                }
            }
        }
    }
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Bool(a), Cell::Bool(b)) => a == b,
            (Cell::Int64(a), Cell::Int64(b)) => a == b,
            (Cell::Float64(a), Cell::Float64(b)) => a == b,
            (Cell::Bytes(a), Cell::Bytes(b)) => a == b,
            _ => todo!(),
        }
    }
}

impl PartialOrd for Cell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Cell::Int64(a), Cell::Int64(b)) => a.partial_cmp(b),
            _ => todo!(),
        }
    }
}

impl Eq for Cell {}

impl Ord for Cell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

// impl Encodeable for Cell {
//     fn encode(&self, writer: &mut SmallWriter) {
//         match self {
//             Cell::Null => todo!(),
//             Cell::Bool(v) => {
//                 writer.write(v);
//             }
//             Cell::Int64(v) => {
//                 writer.write(v);
//             }
//             Cell::Float64(v) => {
//                 writer.write(v);
//             }
//             Cell::Bytes(v) => {
//                 // write size
//                 let size = v.len() as u16;
//                 writer.write(&size);

//                 // write payload
//                 writer.write_bytes(v);
//             }
//         }
//     }
// }

impl Decodeable for Cell {
    fn decode_from<R: std::io::Read>(_reader: &mut R) -> Self {
        todo!()
    }
}
