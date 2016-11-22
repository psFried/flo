use ::{Dot, ElementCounter, ActorId};

use std::io::{self, Read};

pub trait Element: Sized {
    type DataReader: Read;

    fn id(&self) -> Dot;
    fn data_reader(&self) -> Self::DataReader;
}

#[derive(Debug, PartialEq, Clone)]
pub struct OwnedElement {
    pub id: Dot,
    pub data: Vec<u8>
}

impl OwnedElement {
    pub fn new<T: Into<Vec<u8>>>(actor: ActorId, counter: ElementCounter, bytes: T) -> OwnedElement {
        OwnedElement {
            id: Dot::new(actor, counter),
            data: bytes.into(),
        }
    }
}

use std::io::Cursor;

impl <'a> Element for &'a OwnedElement {
    type DataReader = Cursor<&'a [u8]>;

    fn id(&self) -> Dot {
        self.id
    }

    fn data_reader(&self) -> Self::DataReader {
        Cursor::new(&self.data)
    }
}



