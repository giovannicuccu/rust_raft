
/*
la struttura del log la penso in questo modo (sulla scorta di rocks db)
il blocco è di lunghezza fissa, così se un blocco è corrotto posso passare al successivo per la verifica
dentro un blocco ci possono stare n record
se una entry va su più blocchi questi sono consecutivi
ogni record ha un checksum per capire se è valido o meno
 */


use std::io::{BufReader, Read, BufWriter, Write};
use std::fs::{File, OpenOptions};
use std::path::{PathBuf, Path};
use std::io;
use std::convert::TryInto;
use std::time::{SystemTime, UNIX_EPOCH};

const BLOCK_SIZE: u16 = u16::MAX;

const HEADER_SIZE: u8 = 11;

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

/*
implementation taken from
https://github.com/adambcomer/database-engine/blob/master/src/wal.rs
*/
pub struct WriteAheadLog {
    path: PathBuf,
    file: BufWriter<File>,
    current_block: Vec<u8>,
    current_log_number:u32,
}

impl WriteAheadLog {
    pub fn new(dir: &str) -> io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WriteAheadLog { path, file, current_block: vec![], current_log_number:0 })
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(WriteAheadLog {
            path: PathBuf::from(path),
            file,
            current_block: vec![],
            current_log_number:0
        })
    }

    pub fn append_entry(&mut self, mut entry : Vec<u8>) -> io::Result<()> {
        if self.current_block.len()+(HEADER_SIZE as usize) < (BLOCK_SIZE as usize) {
            if self.current_block.len()+(HEADER_SIZE as usize)+entry.len()<= (BLOCK_SIZE as usize) {
                self.append_record(FULL, entry);
            } else {
                let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                let entry_part=entry.drain(0..available_buffer_len).collect();
                self.append_record(FIRST, entry_part);
                self.file.write_all(&self.current_block);
                self.current_block.clear();
                while entry.len()+(HEADER_SIZE as usize)>(BLOCK_SIZE as usize) {
                    let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                    let entry_part=entry.drain(0..available_buffer_len).collect();
                    self.append_record(MIDDLE, entry_part);
                    self.file.write_all(&self.current_block);
                    self.current_block.clear();
                }
                let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                let entry_part=entry.drain(0..available_buffer_len).collect();
                if entry.len()+(HEADER_SIZE as usize)==(BLOCK_SIZE as usize) {
                    self.append_record(MIDDLE, entry_part);
                    self.file.write_all(&self.current_block);
                    self.current_block.clear();
                } else {
                    self.append_record(LAST, entry_part);
                }
            }
        }
        Ok(())
    }

    fn append_record(&mut self, entry_type:u8, mut entry : Vec<u8>) -> io::Result<()> {
        let crc:u8=0;
        self.current_block.append(&mut Vec::from(crc.to_be_bytes()));
        let size:u16= entry.len() as u16;
        self.current_block.append(&mut Vec::from(size.to_be_bytes()));
        self.current_block.append(&mut Vec::from(entry_type.to_be_bytes()));
        self.current_log_number+=1;
        self.current_block.append(&mut Vec::from(self.current_log_number.to_be_bytes()));
        self.current_block.append(&mut entry);
        Ok(())
    }
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