
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
use crc::{Crc, CRC_32_ISCSI};
use crate::common::{TermType, IndexType};

const BLOCK_SIZE: u16 = 32768;

const HEADER_SIZE: u8 = 15;

const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;

const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

struct RecordEntry {
    crc: u32,
    size: u16,
    entry_type: u8,
    term: TermType,
    index: IndexType,
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

        Ok(WriteAheadLog { path, file, current_block: vec![]})
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(WriteAheadLog {
            path: PathBuf::from(path),
            file,
            current_block: vec![]
        })
    }

    pub fn append_entry(&mut self, term: TermType, index: IndexType, mut entry : Vec<u8>) -> io::Result<()> {
        /*
        TODO: pensare posso scrivere una entry vuota?
         */
        if self.current_block.len()+(HEADER_SIZE as usize) <= (BLOCK_SIZE as usize) {
            if self.current_block.len()+(HEADER_SIZE as usize)+entry.len()<= (BLOCK_SIZE as usize) {
                println!("writing record<block_size");
                self.append_record(FULL, term, index,  entry)?;
            } else {
                let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                let entry_part=entry.drain(0..available_buffer_len).collect();
                self.append_record(FIRST, term, index,entry_part)?;
                self.file.write_all(&self.current_block)?;
                self.current_block.clear();
                while entry.len()+(HEADER_SIZE as usize)>(BLOCK_SIZE as usize) {
                    let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                    let entry_part=entry.drain(0..available_buffer_len).collect();
                    self.append_record(MIDDLE, term, index,entry_part)?;
                    self.file.write_all(&self.current_block)?;
                    self.current_block.clear();
                }
                self.append_record(LAST, term, index,entry)?;
                /*let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                let entry_part=entry.drain(0..available_buffer_len).collect();
                if entry.len()+(HEADER_SIZE as usize)==(BLOCK_SIZE as usize) {
                    self.append_record(MIDDLE, entry_part);
                    self.file.write_all(&self.current_block);
                    self.current_block.clear();
                } else {
                    self.append_record(LAST, entry);
                }*/
            }
        }
        /*
        TODO aggiungere gestione errore per else che non dovrebbe mai verificarsi
         */
        Ok(())
    }

    fn append_record(&mut self, entry_type:u8, term: TermType, index: IndexType, mut entry : Vec<u8>) -> io::Result<()> {
        let crc:u32=CASTAGNOLI.checksum(&entry);
        self.current_block.append(&mut Vec::from(crc.to_le_bytes()));
        let size:u16= entry.len() as u16;
        self.current_block.append(&mut Vec::from(size.to_le_bytes()));
        self.current_block.append(&mut Vec::from(entry_type.to_le_bytes()));
        self.current_block.append(&mut Vec::from(term.to_le_bytes()));
        self.current_block.append(&mut Vec::from(index.to_le_bytes()));
        self.current_block.append(&mut entry);
        let slice=self.current_block.as_slice();
        println!("size1={},size2={}",slice[4],slice[5]);
        println!("wrote crc={},size={},entry_type={},index={}",crc,size, entry_type,index);
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.current_block.len()>0 {
            while  self.current_block.len()< BLOCK_SIZE as usize {
                self.current_block.push(0);

            }
            self.file.write_all(&self.current_block)?;
            self.current_block.clear();
        }
        self.file.flush()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub const fn block_size() ->u16 {
        BLOCK_SIZE
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
        if self.current_buffer.len()<HEADER_SIZE as usize  {
            let mut new_buffer=[0; BLOCK_SIZE as usize];
            if self.reader.read_exact(&mut new_buffer).is_err() {
                println!("next_record is err first_read");
                return None;
            }
            self.current_buffer=Vec::from(new_buffer);
        }
        if self.current_buffer.len()>=HEADER_SIZE as usize  {
            let crc = u32::from_le_bytes(self.current_buffer.drain(0..4).collect::<Vec<u8>>().try_into().expect("crc sub array with incorrect length"));
            let size = u16::from_le_bytes(self.current_buffer.drain(0..2).collect::<Vec<u8>>().try_into().expect("size sub array with incorrect length"));
            let entry_type = u8::from_le_bytes(self.current_buffer.drain(0..1).collect::<Vec<u8>>().try_into().expect("entry type sub array with incorrect length"));
            let term = u32::from_le_bytes(self.current_buffer.drain(0..4).collect::<Vec<u8>>().try_into().expect("log number sub array with incorrect length"));
            let index = u32::from_le_bytes(self.current_buffer.drain(0..4).collect::<Vec<u8>>().try_into().expect("log number sub array with incorrect length"));
            let value=self.current_buffer.drain(0..size as usize).collect::<Vec<u8>>();
            let calculated_crc=CASTAGNOLI.checksum(&value);
            if crc!=calculated_crc {
                println!("next_record crc err  crc={}, calculated_crc={} size={},entry_type={},index={}", crc, calculated_crc, size,entry_type,index);
            }
            return if crc == calculated_crc {
                Some(RecordEntry {
                    crc,
                    size,
                    entry_type,
                    term,
                    index,
                    value,
                })
            } else {
                None
            }
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