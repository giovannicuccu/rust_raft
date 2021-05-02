
/*
la struttura del log la penso in questo modo (sulla scorta di rocks db)
il blocco è di lunghezza fissa, così se un blocco è corrotto posso passare al successivo per la verifica
dentro un blocco ci possono stare n record
se una entry va su più blocchi questi sono consecutivi
ogni record ha un checksum per capire se è valido o meno
 */


use std::io::{BufReader, Read, BufWriter, Write, Seek, SeekFrom, Error, ErrorKind};
use std::fs::{File, OpenOptions, metadata};
use std::path::{PathBuf, Path};
use std::{io, env, fs};
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
    writer: BufWriter<File>,
    blocks_written: u32,
    current_block: Vec<u8>,
}

pub struct WriteAheadLogEntry {
    term: TermType,
    index: IndexType,
    data: Vec<u8>,
}

impl WriteAheadLogEntry {
    pub fn new(term: u32, index: u32, data: Vec<u8>) -> Self {
        WriteAheadLogEntry { term, index, data }
    }
    pub fn term(&self) -> u32 {
        self.term
    }
    pub fn index(&self) -> u32 {
        self.index
    }
    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }
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

        Ok(WriteAheadLog { path, writer: file, blocks_written: 0, current_block: vec![]})
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: &str) -> io::Result<Self> {

        let mut log_reader=RecordEntryIterator::new(PathBuf::from(path)).unwrap();
        while let Some(_wal_entry)=log_reader.next(){ println!("wal_read buf_len {}",log_reader.current_buffer.len());}
        let blocks_read=log_reader.blocks_read;
        println!("wal_read buf_len {}",log_reader.current_buffer.len());
        let offset_in_block=(BLOCK_SIZE - log_reader.current_buffer.len() as u16) as usize;
        println!("blocks_read={} offset in block={}, buf_len={}", blocks_read, offset_in_block,log_reader.current_buffer.len());


        let mut file = OpenOptions::new().read(true).write(true).open(PathBuf::from(path))?;
        let mut current_block = vec![];
        let mut seek_position = ((blocks_read) * BLOCK_SIZE as u32) as u64;
        if offset_in_block!= BLOCK_SIZE as usize {
            seek_position = ((blocks_read-1) * BLOCK_SIZE as u32) as u64;
            file.seek(SeekFrom::Start(seek_position));
            let mut new_buffer = [0; BLOCK_SIZE as usize];
            {
                let mut reader = BufReader::new(file);

                if reader.read_exact(&mut new_buffer).is_err() {
                    return Err(Error::new(ErrorKind::Other, "unable to read from file ".to_owned() + path));
                }
            }
            current_block = Vec::from(new_buffer);
            current_block.truncate(offset_in_block);
        }

        let mut file = OpenOptions::new().read(true).write(true).open(PathBuf::from(path))?;
        file.seek(SeekFrom::Start(seek_position));
        let writer = BufWriter::new(file);
        Ok(WriteAheadLog {
            path: PathBuf::from(path),
            writer,
            blocks_written: blocks_read-1,
            current_block
        })
    }

    pub fn append_entry(&mut self, mut entry: WriteAheadLogEntry ) -> io::Result<()> {
        if entry.data.len()==0 {
            return Err(Error::new(ErrorKind::Other, "The entry data is empty"));
        }
        if self.current_block.len()+(HEADER_SIZE as usize) <= (BLOCK_SIZE as usize) {
            if self.current_block.len()+(HEADER_SIZE as usize)+entry.data.len()<= (BLOCK_SIZE as usize) {
                println!("writing record<block_size");
                self.append_record(FULL, entry.term, entry.index,  entry.data)?;
                if self.current_block.len()== (BLOCK_SIZE as usize) {
                    self.writer.write_all(&self.current_block)?;
                    self.current_block.clear();
                    self.blocks_written+=1;
                }
            } else {
                let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                let entry_part=entry.data.drain(0..available_buffer_len).collect();
                self.append_record(FIRST, entry.term, entry.index,entry_part)?;
                self.writer.write_all(&self.current_block)?;
                self.current_block.clear();
                self.blocks_written+=1;
                while entry.data.len()+(HEADER_SIZE as usize)>(BLOCK_SIZE as usize) {
                    let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                    let entry_part=entry.data.drain(0..available_buffer_len).collect();
                    self.append_record(MIDDLE, entry.term, entry.index,entry_part)?;
                    self.writer.write_all(&self.current_block)?;
                    self.current_block.clear();
                    self.blocks_written+=1;
                }
                if entry.data.len()>0 {
                    self.append_record(LAST, entry.term, entry.index, entry.data)?;
                    if self.current_block.len() == (BLOCK_SIZE as usize) {
                        self.writer.write_all(&self.current_block)?;
                        self.current_block.clear();
                        self.blocks_written += 1;
                    }
                }
            }
        } else {
            println!("self.current_block.len()={}",self.current_block.len());
            return Err(Error::new(ErrorKind::Other, "Internal error incorrect current_block size "));
        }
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
        println!("wrote crc={},size={},entry_type={},index={}",crc,size, entry_type,index);
        Ok(())
    }

    pub fn seek_and_clear_after(&mut self, term: TermType, index: IndexType) -> io::Result<()> {
        self.flush();
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        //println!("path={}", self.path.display());
        let mut log_reader=RecordEntryIterator::new(self.path.clone()).unwrap();
        let mut found=false;
        while let Some(wal_entry)=log_reader.next(){
            /*
           TODO: spiegare che iteratore consuma e ciclo wile no
             */
        //for wal_entry in log_reader {
            //println!("entry term={}, index={}", wal_entry.term, wal_entry.index);
            if wal_entry.term==term && wal_entry.index==index {
                //println!("entry found");
                found=true;
                break;
            }
        }
        if !found {
            return Err(Error::new(ErrorKind::Other, "Term and index not found"));
        }
        let blocks_read=log_reader.blocks_read;
        let offset_in_block=(BLOCK_SIZE - log_reader.current_buffer.len() as u16) as usize;
        let new_file_size=((blocks_read-1) * BLOCK_SIZE as u32) as u64;
        file.seek(SeekFrom::Start(new_file_size));

        self.current_block.clear();
        let mut buffer = vec![0u8; offset_in_block];
        //println!("buffer_size={}",offset_in_block);
        file.read_exact(&mut *buffer)?;
        self.current_block.append(&mut buffer);
        file.set_len(new_file_size)?;
        //println!("new_file_size={}",new_file_size);
        file.seek(SeekFrom::Start(new_file_size));
        self.writer = BufWriter::new(file);
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.current_block.len()>0 {
            let mut padded_with_zero=false;
            let non_padded_len=self.current_block.len();
            if self.current_block.len()< BLOCK_SIZE as usize {
                padded_with_zero=true;
                while self.current_block.len() < BLOCK_SIZE as usize {
                    self.current_block.push(0);
                }
            }
            self.writer.write_all(&self.current_block)?;
            if padded_with_zero {
                let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
                let seek_position = ((self.blocks_written) * BLOCK_SIZE as u32) as u64;
                file.seek(SeekFrom::Start(seek_position));
                self.writer = BufWriter::new(file);
                self.current_block.truncate(non_padded_len);
            } else {
                self.current_block.clear();
                self.blocks_written+=1;
            }

        }
        self.writer.flush()
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
    blocks_read: u32,
}

impl RecordEntryIterator {
    /// Creates a new RecordEntryIterator from a path to a Log file.
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(RecordEntryIterator { reader, current_buffer: vec![],blocks_read: 0 })
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
            self.blocks_read+=1;
            self.current_buffer=Vec::from(new_buffer);
        }
        if self.current_buffer.len()>=HEADER_SIZE as usize  {
            let slice=&self.current_buffer[0..15];
            let mut contains_entry=false;
            for i in 0..15 {
                if slice[i]!=0 {
                    contains_entry=true;
                    break;
                }
            }
            if !contains_entry {
                return None;
            }
            let crc = u32::from_le_bytes(self.current_buffer.drain(0..4).collect::<Vec<u8>>().try_into().expect("crc sub array with incorrect length"));
            let size = u16::from_le_bytes(self.current_buffer.drain(0..2).collect::<Vec<u8>>().try_into().expect("size sub array with incorrect length"));

            let entry_type = u8::from_le_bytes(self.current_buffer.drain(0..1).collect::<Vec<u8>>().try_into().expect("entry type sub array with incorrect length"));
            let term = u32::from_le_bytes(self.current_buffer.drain(0..4).collect::<Vec<u8>>().try_into().expect("term sub array with incorrect length"));
            let index = u32::from_le_bytes(self.current_buffer.drain(0..4).collect::<Vec<u8>>().try_into().expect("index sub array with incorrect length"));
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
    type Item = WriteAheadLogEntry;

    /// Gets the next entry in the WAL file.
    fn next(&mut self) -> Option<WriteAheadLogEntry> {
        let mut log_value: Vec<u8>=Vec::new();
        //Posso farlo con una ma lo faccio con due per maggiore chiarezza
        let mut expect_first_or_full=true;
        let mut expect_middle_or_last=false;
        while let Some(mut actual_record) = self.next_record() {
                log_value.append(&mut actual_record.value);
                if actual_record.entry_type==FULL {
                    return if expect_first_or_full {
                        Some(WriteAheadLogEntry{ term:actual_record.term, index: actual_record.index, data:log_value})
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
                       Some(WriteAheadLogEntry{ term:actual_record.term, index: actual_record.index, data:log_value})
                   } else {
                       None
                   }
               }
            }
        None
    }
}