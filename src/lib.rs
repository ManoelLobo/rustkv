use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use serde_derive::{Deserialize, Serialize};

type ByteString = Vec<u8>;

type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    key: ByteString,
    value: ByteString,
}

#[derive(Debug)]
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

    fn process_record<R: Read>(file: &mut R) -> io::Result<KeyValuePair> {
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

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?;

        self.index.insert(key.to_vec(), position);

        Ok(())
    }

    fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let mut file = BufWriter::new(&mut self.file);
        let key_length = key.len();
        let value_length = value.len();
        let mut tmp = ByteString::with_capacity(key_length + value_length);

        for byte in key {
            tmp.push(*byte);
        }

        for byte in value {
            tmp.push(*byte);
        }

        let checksum = crc32::checksum_ieee(&tmp);

        let next_byte = SeekFrom::End(0);
        let position = file.seek(SeekFrom::Current(0))?;
        file.seek(next_byte)?;
        file.write_u32::<LittleEndian>(checksum)?;
        file.write_u32::<LittleEndian>(key_length as u32)?;
        file.write_u32::<LittleEndian>(value_length as u32)?;
        file.write_all(&tmp)?;

        Ok(position)
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            Some(position) => *position,
            None => return Ok(None),
        };

        let kv = self.get_at_position(position)?;

        Ok(Some(kv.value))
    }

    fn get_at_position(&mut self, position: u64) -> io::Result<KeyValuePair> {
        let mut file = BufReader::new(&self.file);
        file.seek(SeekFrom::Start(position))?;

        RustKV::process_record(&mut file)
    }

    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }
}
