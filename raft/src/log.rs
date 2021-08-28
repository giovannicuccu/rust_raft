
/*
la struttura del log la penso in questo modo (sulla scorta di rocks db)
il blocco è di lunghezza fissa, così se un blocco è corrotto posso passare al successivo per la verifica
dentro un blocco ci possono stare n record
se una entry va su più blocchi questi sono consecutivi
ogni record ha un checksum per capire se è valido o meno
 */

/*
TODO: aggiungere la modalità che consente di scegliere fra max_security, max_performance e se ci sta balanced che ogni tot entry o tot secondi fa la sync
TODO: test con criterion
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

pub const HEADER_SIZE: u8 = 15;

const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;

const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);


const FORMAT_VERSION: u8 = 1;

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
    writer: File,
    blocks_written: u32,
    current_block: Vec<u8>,
    current_index: IndexType,
    flushed_partial_block:bool,
}

pub struct WriteAheadLogEntry {
    index: IndexType,
    term: TermType,
    data: Vec<u8>,
}

impl WriteAheadLogEntry {
    pub fn new(index: IndexType, term: TermType, data: Vec<u8>) -> Self {
        WriteAheadLogEntry { index, term, data }
    }
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }
    pub fn term(&self) -> TermType {
        self.term
    }
}


impl WriteAheadLog {
    pub fn new(dir: &str) -> io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let mut file = OpenOptions::new().append(true).create(true).open(&path)?;
        //let file = BufWriter::new(file);
        //come posso fare per usare direttamente  FORMAT_VERSION?
        let version_buffer=[FORMAT_VERSION; 1];
        file.write_all(&version_buffer);
        file.flush();
        Ok(WriteAheadLog { path, writer: file, blocks_written: 0, current_block: vec![],current_index:0, flushed_partial_block:false})
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: &str) -> io::Result<Self> {

        let mut log_reader=RecordEntryIterator::new(PathBuf::from(path),vec![], false).unwrap();
        let mut last_index=0;
        while let Some(wal_entry)=log_reader.next(){
            println!("wal_read buf_len {}",log_reader.current_buffer_len());
            last_index=wal_entry.index;
        }
        let blocks_read=log_reader.blocks_read;
        println!("wal_read buf_len {}",log_reader.current_buffer_len());
        let offset_in_block=(BLOCK_SIZE - log_reader.current_buffer_len() as u16) as usize;
        println!("blocks_read={} offset in block={}, buf_len={}", blocks_read, offset_in_block,log_reader.current_buffer_len());


        let mut file = OpenOptions::new().read(true).write(true).open(PathBuf::from(path))?;
        let mut version_buffer = [0; 1];
        file.read_exact(&mut version_buffer)?;
        let version_format=u8::from_le_bytes(version_buffer);
        if version_format!=FORMAT_VERSION {
            return Err(Error::new(ErrorKind::Other, "Wrong file version format: not 1"));
        }
        let mut current_block = vec![];
        //aggiungo 1 perchè il primo byte è la versione
        let mut seek_position = ((blocks_read) * BLOCK_SIZE as u32) as u64+1;
        if offset_in_block!= BLOCK_SIZE as usize {
            //aggiungo 1 perchè il primo byte è la versione
            seek_position = ((blocks_read-1) * BLOCK_SIZE as u32) as u64+1;

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
        let mut blocks_written=0;
        if blocks_read>0 {
            blocks_written=blocks_read-1;
        }
        Ok(WriteAheadLog {
            path: PathBuf::from(path),
            writer: file,
            blocks_written,
            current_block,
            current_index:last_index,
            flushed_partial_block:false,
        })
    }

    pub fn append_entry(&mut self, term: TermType, entry : &Vec<u8> ) -> io::Result<IndexType> {
        if entry.len()==0 {
            return Err(Error::new(ErrorKind::Other, "The entry data is empty"));
        }
        let mut entry_data:Vec<u8> =Vec::new();
        entry_data.extend(&entry[0..entry.len()]);
        self.current_index+=1;
        if self.current_block.len()+(HEADER_SIZE as usize) <= (BLOCK_SIZE as usize) {
            if self.current_block.len()+(HEADER_SIZE as usize)+entry_data.len()<= (BLOCK_SIZE as usize) {
                println!("writing record<block_size");
                self.append_record(FULL, term, self.current_index,  entry_data)?;
            } else {
                let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                let entry_part=entry_data.drain(0..available_buffer_len).collect();
                self.append_record(FIRST, term, self.current_index,entry_part)?;
                while entry_data.len()+(HEADER_SIZE as usize)>(BLOCK_SIZE as usize) {
                    let available_buffer_len=(BLOCK_SIZE as usize)-self.current_block.len()-(HEADER_SIZE as usize);
                    let entry_part=entry_data.drain(0..available_buffer_len).collect();
                    self.append_record(MIDDLE, term, self.current_index,entry_part)?;
                }
                if entry_data.len()>0 {
                    self.append_record(LAST, term, self.current_index, entry_data)?;
                }
            }
        } else {
            println!("self.current_block.len()={}",self.current_block.len());
            return Err(Error::new(ErrorKind::Other, "Internal error incorrect current_block size "));
        }
        Ok(self.current_index)
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
        if self.current_block.len()== (BLOCK_SIZE as usize) {
            self.writer.write_all(&self.current_block)?;
            self.current_block.clear();
            self.flush();
            self.blocks_written+=1;
        }
        Ok(())
    }

    pub fn seek_and_clear_after(&mut self, index: IndexType) -> io::Result<()> {
        self.flush();
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        //println!("path={}", self.path.display());
        let mut log_reader=RecordEntryIterator::new(self.path.clone(),vec![], false).unwrap();
        let mut found=false;
        while let Some(wal_entry)=log_reader.next(){
            /*
           TODO: spiegare che iteratore consuma e ciclo wile no
             */
        //for wal_entry in log_reader {
            //println!("entry term={}, index={}", wal_entry.term, wal_entry.index);
            if wal_entry.index==index {
                //println!("entry found");
                found=true;
                break;
            }
        }
        if !found {
            return Err(Error::new(ErrorKind::Other, "Term and index not found"));
        }
        let blocks_read=log_reader.blocks_read;
        let offset_in_block=(BLOCK_SIZE - log_reader.current_buffer_len() as u16) as usize;
        let new_file_size=((blocks_read-1) * BLOCK_SIZE as u32) as u64+1;
        file.seek(SeekFrom::Start(new_file_size));

        self.current_block.clear();
        let mut buffer = vec![0u8; offset_in_block];
        println!("buffer_size={}",offset_in_block);
        file.read_exact(&mut *buffer)?;
        self.current_block.append(&mut buffer);
        file.set_len(new_file_size)?;
        println!("new_file_size={}",new_file_size);
        file.seek(SeekFrom::Start(new_file_size));
        self.writer = file;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.current_block.len()>0 {
            let mut padded_with_zero=false;
            let non_padded_len=self.current_block.len();
            println!("non_padded_len={}",non_padded_len);
            if self.current_block.len()< BLOCK_SIZE as usize {
                padded_with_zero=true;
                self.current_block.resize(BLOCK_SIZE as usize, 0);
            }
            self.writer.write_all(&self.current_block)?;
            if padded_with_zero {
                let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
                let seek_position = ((self.blocks_written) * BLOCK_SIZE as u32) as u64+1;
                file.seek(SeekFrom::Start(seek_position));
                self.writer = file;
                self.current_block.truncate(non_padded_len);
                self.flushed_partial_block=true;
            } else {
                self.current_block.clear();
                self.blocks_written+=1;
                self.flushed_partial_block=false;
            }

        }
        self.writer.flush()
    }

    pub fn last_entry(&self) -> Option<WriteAheadLogEntry> {
        let mut iterator=self.record_entry_iterator().unwrap();
        let mut last_entry=None;
        while let Some(log_entry) = iterator.next() {
            last_entry=Some(log_entry);
        }
        last_entry
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub const fn block_size() ->u16 {
        BLOCK_SIZE
    }

    pub fn record_entry_iterator(&self) -> io::Result<RecordEntryIterator> {
        RecordEntryIterator::new(self.path.clone(), self.current_block.clone(), self.flushed_partial_block)
    }
}


pub struct RecordEntryIterator {
    reader: File,
    current_buffer: Vec<u8>,
    blocks_to_read: u32,
    blocks_read: u32,
    in_memory_fragment: Vec<u8>,
    reading_in_memory: bool,
}

impl RecordEntryIterator {

    fn new(path: PathBuf, in_memory_fragment: Vec<u8>,flushed_partial_block: bool) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path.clone())?;
        let mut version_buffer = [0; 1];
        file.read_exact(&mut version_buffer)?;
        let version_format=u8::from_le_bytes(version_buffer);
        if version_format!=FORMAT_VERSION {
            return Err(Error::new(ErrorKind::Other, "Wrong file version format: not 1"));
        }
        let file_metadata = metadata(path.clone());
        let mut blocks_to_read= (file_metadata.unwrap().len()/BLOCK_SIZE as u64) as u32;
        let file_metadata = metadata(path);
        println!("file_size {}", file_metadata.unwrap().len());
        if flushed_partial_block {
            println!("flushed_partial_block true");
            blocks_to_read=blocks_to_read-1;
        }
        println!("blocks to read {}", blocks_to_read);
        Ok(RecordEntryIterator { reader: file, current_buffer:vec![], blocks_to_read , blocks_read: 0,in_memory_fragment,reading_in_memory:false  })
    }
}

fn read_from_vec(vec: &mut Vec<u8>)-> Option<RecordEntry> {
    println!("vec.len()={}",vec.len());
    if vec.len() >= HEADER_SIZE as usize {
        let slice = &vec[0..HEADER_SIZE as usize];
        let mut contains_entry = false;
        for i in 0..HEADER_SIZE as usize {
            if slice[i] != 0 {
                contains_entry = true;
                break;
            }
        }
        if !contains_entry {
            println!("No entry");
            return None;
        }
        let crc = u32::from_le_bytes(vec.drain(0..4).collect::<Vec<u8>>().try_into().expect("crc sub array with incorrect length"));
        let size = u16::from_le_bytes(vec.drain(0..2).collect::<Vec<u8>>().try_into().expect("size sub array with incorrect length"));

        let entry_type = u8::from_le_bytes(vec.drain(0..1).collect::<Vec<u8>>().try_into().expect("entry type sub array with incorrect length"));
        let term = u32::from_le_bytes(vec.drain(0..4).collect::<Vec<u8>>().try_into().expect("term sub array with incorrect length"));
        let index = u32::from_le_bytes(vec.drain(0..4).collect::<Vec<u8>>().try_into().expect("index sub array with incorrect length"));
        let value = vec.drain(0..size as usize).collect::<Vec<u8>>();
        let calculated_crc = CASTAGNOLI.checksum(&value);
        if crc != calculated_crc {
            println!("next_record crc err  crc={}, calculated_crc={} size={},entry_type={},index={}", crc, calculated_crc, size, entry_type, index);
        }
        return if crc == calculated_crc {
            println!("read_from_vec return some");
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

impl RecordEntryIterator {
    pub fn seek(&mut self, index: IndexType) -> io::Result<WriteAheadLogEntry>{
        if self.in_memory_fragment.len()>=HEADER_SIZE as usize {
            let mut pos_in_buffer=0;
            while pos_in_buffer+(HEADER_SIZE as usize)<self.in_memory_fragment.len() {
                let slice = &self.in_memory_fragment[0..HEADER_SIZE as usize];
                let mut contains_entry = false;
                for i in 0..HEADER_SIZE as usize {
                    if slice[i] != 0 {
                        contains_entry = true;
                        break;
                    }
                }
                if !contains_entry {
                    break;
                }
                let actual_term = u32::from_le_bytes(self.in_memory_fragment[pos_in_buffer + 7..pos_in_buffer +11].try_into().unwrap());
                let actual_index = u32::from_le_bytes(self.in_memory_fragment[pos_in_buffer + 11..pos_in_buffer +15].try_into().unwrap());
                let size = u16::from_le_bytes(self.in_memory_fragment[pos_in_buffer + 4..pos_in_buffer +6].try_into().unwrap());
                let entry_type = u8::from_le_bytes(self.in_memory_fragment[pos_in_buffer + 6..pos_in_buffer +7].try_into().unwrap());
                let data = self.in_memory_fragment[pos_in_buffer + 15..pos_in_buffer + 15 +size as usize].to_owned();
                if actual_index < index {
                    pos_in_buffer+=(HEADER_SIZE as u16 +size) as usize;

                } else if actual_index==index && (entry_type==FULL || entry_type==LAST) {
                    //controllo su entry type ridondante presente solo per scruopolo
                    self.in_memory_fragment.drain(0..pos_in_buffer+HEADER_SIZE as usize +size as usize);
                    self.reading_in_memory=true;
                    return Ok(WriteAheadLogEntry{ index, term: actual_term, data });
                } else {
                    break;
                }
            }

        }
        let mut found=false;
        while let Some(wal_entry)=self.next(){
            if wal_entry.index==index {
                return Ok(wal_entry)
            }
        }
        Err(Error::new(ErrorKind::Other, "could not find term and index"))

    }

    fn next_record(&mut self) -> Option<RecordEntry> {
        if !self.reading_in_memory {
            if self.current_buffer.len() < HEADER_SIZE as usize {
                if self.in_memory_fragment.len() > 0 && self.blocks_to_read>0 && self.blocks_read == (self.blocks_to_read) {
                    self.reading_in_memory = true;
                } else {
                    let mut new_buffer = [0; BLOCK_SIZE as usize];
                    if self.reader.read_exact(&mut new_buffer).is_err() {
                        println!("next_record is err first_read");
                        if self.in_memory_fragment.len() > 0 {
                            self.reading_in_memory = true;
                        } else {
                            return None;
                        }
                    } else {
                        println!("read exact ok");
                        self.blocks_read += 1;
                        self.current_buffer = Vec::from(new_buffer);
                    }
                }
            }
            if self.reading_in_memory {
                println!("reading from memory");
                return read_from_vec(&mut self.in_memory_fragment);
            }
            println!("reading from current buffer");
            return read_from_vec(&mut self.current_buffer);
            /*return if opt_entry.is_none() {
                if self.in_memory_fragment.len() > 0 {
                    self.reading_in_memory = true;
                    println!("reading from memory after reading all from disk");
                    read_from_vec(&mut self.in_memory_fragment)
                } else {
                    println!("returning none memfragment=0");
                    None
                }
            } else {
                println!("returning from file");
                opt_entry
            }*/

        }
        println! ("reading from memory final");
        read_from_vec(&mut self.in_memory_fragment)
    }

    fn current_buffer_len(&self) ->usize {
        if self.reading_in_memory {
            self.in_memory_fragment.len()
        } else {
            self.current_buffer.len()
        }
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
                println!("actual_record.entry_type {} ",actual_record.entry_type);
                if actual_record.entry_type==FULL {
                    return if expect_first_or_full {
                        Some(WriteAheadLogEntry{ index: actual_record.index, term:actual_record.term, data:log_value})
                    } else {
                        println!("expect_first_or_full failed for full");
                        None
                    }
                }
               if actual_record.entry_type==FIRST {
                   if expect_first_or_full {
                       expect_first_or_full = false;
                       expect_middle_or_last = true;
                   } else {
                       println!("expect_first_or_full failed for first ");
                       return None;
                   }
               }
               if actual_record.entry_type==MIDDLE {
                   if !expect_middle_or_last {
                       println!("expect_middle_or_last failed for middle ");
                       return None;
                   }
               }
               if actual_record.entry_type==LAST {
                   return if expect_middle_or_last {
                       Some(WriteAheadLogEntry{ index: actual_record.index, term:actual_record.term, data:log_value})
                   } else {
                       println!("expect_middle_or_last failed for last ");
                       None
                   }
               }
            }
        println!("next_record none ");
        None
    }
}