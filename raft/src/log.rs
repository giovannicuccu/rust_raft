
/*
la struttura del log la penso in questo modo (sulla scorta di rocks db)
il blocco è di lunghezza fissa, così se un blocco è corrotto posso passare al successivo per la verifica
dentro un blocco ci possono stare n record
se una entry va su più blocchi questi sono consecutivi
ogni record ha un checksum per capire se è valido o meno
 */


use std::io::{BufReader, Read};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::io;
use std::convert::TryInto;

const BLOCK_SIZE: u16 = u16::MAX;

const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;

struct RecordEntry {
    crc: u32,
    size: u16,
    entry_type: u8,
    log_number: u32,
    value: Vec<u8>,
}

pub struct RecordEntryIterator {
    reader: BufReader<File>,
    current_buffer: Vec<u8>,
}

impl RecordEntryIterator {
    /// Creates a new RecordEntryIterator from a path to a Log file.
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(RecordEntryIterator { reader, current_buffer: vec![] })
    }
}


impl RecordEntryIterator {
    fn next_record(&mut self) -> Option<RecordEntry> {
        if self.current_buffer.len()<11  {
            let mut new_buffer=[0; BLOCK_SIZE as usize];
            if self.reader.read_exact(&mut new_buffer).is_err() {
                return None;
            }
            self.current_buffer=Vec::from(new_buffer);
        }
        if self.current_buffer.len()>11  {
            let crc = u32::from_le_bytes(self.current_buffer.drain(0..4).collect::<Vec<u8>>().try_into().expect("crc sub array with incorrect length"));
            let size = u16::from_le_bytes(self.current_buffer.drain(4..6).collect::<Vec<u8>>().try_into().expect("crc sub array with incorrect length"));
            let entry_type = u8::from_le_bytes(self.current_buffer.drain(6..7).collect::<Vec<u8>>().try_into().expect("crc sub array with incorrect length"));
            let log_number = u32::from_le_bytes(self.current_buffer.drain(7..11).collect::<Vec<u8>>().try_into().expect("crc sub array with incorrect length"));
            let value=self.current_buffer.drain(11..(11+size) as usize).collect::<Vec<u8>>();
            return Some(RecordEntry {
                crc,
                size,
                entry_type,
                log_number,
                value,
            });
        }
        None
    }
}

impl Iterator for RecordEntryIterator {
    type Item = Vec<u8>;

    /// Gets the next entry in the WAL file.
    fn next(&mut self) -> Option<Vec<u8>> {
        let mut log_value: Vec<u8>=Vec::new();
        //Posso farlo con una ma lo faccio con due per maggiore chiarezza
        let mut expect_first_or_full=true;
        let mut expect_middle_or_last=false;
        while let Some(mut actual_record) = self.next_record() {
                log_value.append(&mut actual_record.value);
                if actual_record.entry_type==FULL {
                    return if expect_first_or_full {
                        Some(log_value)
                    } else {
                        None
                    }
                }
               if actual_record.entry_type==FIRST {
                   if expect_first_or_full {
                       expect_first_or_full = false;
                       expect_middle_or_last = true;
                   } else {
                       return None;
                   }
               }
               if actual_record.entry_type==MIDDLE {
                   if !expect_middle_or_last {
                       return None;
                   }
               }
               if actual_record.entry_type==LAST {
                   return if expect_middle_or_last {
                       Some(log_value)
                   } else {
                       None
                   }
               }
            }
        None
    }
}