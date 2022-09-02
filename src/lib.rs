use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use byteorder::{LittleEndian, ReadBytesExt};
use crc::crc32;

type ByteString = Vec<u8>;

type ByteStr = [u8];

pub struct KeyValuePair {
    key: ByteString,
    value: ByteString,
}

pub struct RustKV {
    file: File,
    pub index: HashMap<ByteString, u64>,
}

impl RustKV {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let index = HashMap::new();

        Ok(Self { file, index })
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut file = BufReader::new(&self.file);

        loop {
            let position = file.seek(SeekFrom::Current(0))?;
            let kv_if_available = RustKV::process_record(&mut file);

            let kv = match kv_if_available {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(err),
                },
            };

            self.index.insert(kv.key, position);
        }

        Ok(())
    }

    pub fn process_record<R: Read>(file: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = file.read_u32::<LittleEndian>()?;
        let key_length = file.read_u32::<LittleEndian>()?;
        let value_length = file.read_u32::<LittleEndian>()?;
        let data_length = key_length + value_length;

        let mut data = ByteString::with_capacity(data_length as usize);

        {
            file.by_ref()
                .take(data_length as u64)
                .read_to_end(&mut data)?;
        }

        debug_assert_eq!(data_length as usize, data.len());

        let checksum = crc32::checksum_ieee(&data);

        if checksum != saved_checksum {
            panic!(
                "Checksum mismatch - data corrupted - ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }

        let value = data.split_off(key_length as usize);
        let key = data;

        Ok(KeyValuePair { key, value })
    }
}
