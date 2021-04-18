use rand::Rng;
use std::fs::{create_dir, OpenOptions, metadata};
use std::io::BufReader;
use raft::log::{WriteAheadLog, RecordEntryIterator};
use std::convert::TryInto;
use std::{env, fs};

fn createVectorDataforTest(len: u32) -> Vec<u8> {
    let mut data: Vec<u8>=Vec::new();
    for i in 0..len {
        data.push(0);
    }
    data[0]=1;
    data[1]=2;
    data[2]=3;
    data[3]=4;
    data
}

fn testReadAndWriteSingleBlock(inputDataLen: u32, expected_wal_blocks: u16) {
    let mut dir = env::temp_dir();
    let mut rng = rand::thread_rng();
    let dir = format!("{}{}",dir.display(), rng.gen::<u32>());
    println!("dir={}",dir);
    create_dir(&dir).unwrap();

    let data1: Vec<u8>= createVectorDataforTest(inputDataLen);

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();
    wal.append_entry(data1);
    wal.flush();

    let file_metadata = metadata(&wal.path());

    let expected_wal_size = WriteAheadLog::block_size() as u64 * expected_wal_blocks as u64;
    assert_eq!(file_metadata.unwrap().len(), expected_wal_size as u64);

    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut reader = BufReader::new(file);
    let mut log_reader=RecordEntryIterator::new(wal.path().clone()).unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let mut data: Vec<u8>= createVectorDataforTest(inputDataLen);
    assert_eq!(data_read,data);
    println!("before getting none");
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}


#[test]
fn testWriteAndReadOneRecord() {
    testReadAndWriteSingleBlock(4 as u32, 1);
}

#[test]
fn testWriteAndReadTwoRecord() {

    let mut dir = env::temp_dir();
    let mut rng = rand::thread_rng();
    let dir = format!("{}{}",dir.display(), rng.gen::<u32>());
    println!("dir={}",dir);
    create_dir(&dir).unwrap();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();
    let data1: Vec<u8>=vec![1,2,3,4];
    wal.append_entry(data1);
    let data2: Vec<u8>=vec![5,6,7,8];
    wal.append_entry(data2);
    wal.flush();
    /*
    TODO capire perchè
    let file_metadata = metadata(&wal.path())?;
    assert_eq!(file_metadata.len() as u16,WriteAheadLog::block_size());
    Non funziona
     */
    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len() as u16,WriteAheadLog::block_size());
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut reader = BufReader::new(file);
    let mut log_reader=RecordEntryIterator::new(wal.path().clone()).unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=vec![1,2,3,4];
    assert_eq!(data_read,data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=vec![5,6,7,8];
    assert_eq!(data_read,data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn testWriteAndReadOneRecordSpanningTwoBlocks() {
    testReadAndWriteSingleBlock(WriteAheadLog::block_size() as u32, 2);
}

#[test]
fn testWriteAndReadOneRecordFillingOneBlock() {
    testReadAndWriteSingleBlock(WriteAheadLog::block_size() as u32-11 as u32, 1);
}

#[test]
fn testWriteAndReadOneRecordOverflowingOneBlock() {
    testReadAndWriteSingleBlock(WriteAheadLog::block_size() as u32-10 as u32, 2);
}

#[test]
fn testWriteAndReadOneRecordSpanningThreeBlocks() {
    testReadAndWriteSingleBlock(WriteAheadLog::block_size() as u32*2 as u32, 3);
}

#[test]
fn testWriteAndReadOneRecordSpanningFourBlocks() {
    testReadAndWriteSingleBlock(WriteAheadLog::block_size() as u32*3 as u32, 4);
}

#[test]
fn testWriteAndReadTwoRecordsWithOnlyHeaderPart() {
    let mut dir = env::temp_dir();
    let mut rng = rand::thread_rng();
    let dir = format!("{}{}",dir.display(), rng.gen::<u32>());
    println!("dir={}",dir);
    create_dir(&dir).unwrap();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();
    let mut array: [u8; WriteAheadLog::block_size()as usize-22 as usize] = [0; WriteAheadLog::block_size()as usize-22 as usize];
    array[0]=1;
    array[1]=2;
    array[3]=3;
    array[4]=4;

    let data1: Vec<u8>=Vec::from(array);
    wal.append_entry(data1);
    let data2: Vec<u8>=vec![5,6,7,8];
    wal.append_entry(data2);
    wal.flush();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize*2 as usize) as u64);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut reader = BufReader::new(file);
    let mut log_reader=RecordEntryIterator::new(wal.path().clone()).unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let mut array: [u8; WriteAheadLog::block_size()as usize-22 as usize] = [0; WriteAheadLog::block_size()as usize-22 as usize];
    array[0]=1;
    array[1]=2;
    array[3]=3;
    array[4]=4;
    let data: Vec<u8>=Vec::from(array);
    assert_eq!(data_read,data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=vec![5,6,7,8];
    assert_eq!(data_read,data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}