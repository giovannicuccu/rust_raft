use rand::Rng;
use std::fs::{create_dir, OpenOptions, metadata};
use std::io::BufReader;
use raft::log::{WriteAheadLog, RecordEntryIterator, WriteAheadLogEntry};
use std::{env};
use std::path::PathBuf;


fn create_test_dir() -> String {
    let dir = env::temp_dir();
    let mut rng = rand::thread_rng();
    let dir = format!("{}{}",dir.display(), rng.gen::<u32>());
    println!("dir={}",dir);
    create_dir(&dir).unwrap();
    dir
}

fn create_vector_data_for_test_with_init_data(len: u32, init_data: Vec<u8>) -> Vec<u8> {
    let mut data: Vec<u8>=Vec::new();
    for _i in 0..len {
        data.push(0);
    }
    for i in 0..init_data.len() {
        data[i]=init_data[i];
    }
    data
}

fn create_vector_data_for_test(len: u32) -> Vec<u8> {
    create_vector_data_for_test_with_init_data(len, vec![1, 2, 3, 4])
}

fn create_vector_data_for_test_01(len: u32) -> Vec<u8> {
    create_vector_data_for_test_with_init_data(len, vec![2, 3, 4, 5])
}

fn create_vector_data_for_test_02(len: u32) -> Vec<u8> {
    create_vector_data_for_test_with_init_data(len, vec![3, 4, 5, 6])
}

fn create_vector_data_for_test_03(len: u32) -> Vec<u8> {
    create_vector_data_for_test_with_init_data(len, vec![4, 5, 6, 7])
}


fn test_read_and_write_single_block(input_data_len: u32, expected_wal_blocks: u16) {
    let dir=create_test_dir();

    let data1: Vec<u8>= create_vector_data_for_test(input_data_len);

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 1,data1)).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());

    let expected_wal_size = WriteAheadLog::block_size() as u64 * expected_wal_blocks as u64;
    assert_eq!(file_metadata.unwrap().len(), expected_wal_size as u64+1);

    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let reader = BufReader::new(file);
    let mut log_reader=wal.record_entry_iterator().unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>= create_vector_data_for_test(input_data_len);
    assert_eq!(*data_read.data(),data);
    println!("before getting none");
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}


#[test]
fn test_write_and_read_one_record() {
    test_read_and_write_single_block(4 as u32, 1);
}

#[test]
fn test_write_and_read_two_record() {

    let dir=create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();
    let data1: Vec<u8>=vec![1,2,3,4];
    wal.append_entry(WriteAheadLogEntry::new(1, 1,data1)).unwrap();
    let data2: Vec<u8>=vec![5,6,7,8];
    wal.append_entry(WriteAheadLogEntry::new(1, 2,data2)).unwrap();
    wal.flush().unwrap();
    /*
    TODO capire perchè
    let file_metadata = metadata(&wal.path())?;
    assert_eq!(file_metadata.len() as u16,WriteAheadLog::block_size());
    Non funziona
     */
    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len() as u16,WriteAheadLog::block_size()+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut log_reader=wal.record_entry_iterator().unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=vec![1,2,3,4];
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=vec![5,6,7,8];
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_write_and_read_four_record() {

    let dir=create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();
    let data1: Vec<u8>=create_vector_data_for_test(100);
    wal.append_entry(WriteAheadLogEntry::new(1, 1,data1)).unwrap();
    let data2: Vec<u8>=create_vector_data_for_test_01(150);
    wal.append_entry(WriteAheadLogEntry::new(1, 2,data2)).unwrap();
    let data3: Vec<u8>=create_vector_data_for_test_02(250);
    wal.append_entry(WriteAheadLogEntry::new(1, 3,data3)).unwrap();
    let data4: Vec<u8>=create_vector_data_for_test_03(350);
    wal.append_entry(WriteAheadLogEntry::new(1, 4,data4)).unwrap();
    wal.flush().unwrap();
    /*
    TODO capire perchè
    let file_metadata = metadata(&wal.path())?;
    assert_eq!(file_metadata.len() as u16,WriteAheadLog::block_size());
    Non funziona
     */
    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len() as u16,WriteAheadLog::block_size()+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let reader = BufReader::new(file);
    let mut log_reader=wal.record_entry_iterator().unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test(100);
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(150);
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_02(250);
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_03(350);
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_write_and_read_one_record_spanning_two_blocks() {
    test_read_and_write_single_block(WriteAheadLog::block_size() as u32, 2);
}

#[test]
fn test_write_and_read_one_record_filling_one_block() {
    test_read_and_write_single_block(WriteAheadLog::block_size() as u32-15 as u32, 1);
}

#[test]
fn test_write_and_read_one_record_overflowing_one_block() {
    test_read_and_write_single_block(WriteAheadLog::block_size() as u32-14 as u32, 2);
}

#[test]
fn test_write_and_read_one_record_spanning_three_blocks() {
    test_read_and_write_single_block(WriteAheadLog::block_size() as u32*2 as u32, 3);
}

#[test]
fn test_write_and_read_one_record_spanning_four_blocks() {
    test_read_and_write_single_block(WriteAheadLog::block_size() as u32*3 as u32, 4);
}

#[test]
fn test_write_and_read_two_records_with_only_header_part() {
    let dir=create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    let data1: Vec<u8>=create_vector_data_for_test((WriteAheadLog::block_size() as usize - 30 as usize) as u32);
    wal.append_entry(WriteAheadLogEntry::new(1, 1,data1)).unwrap();
    let data2: Vec<u8>=vec![5,6,7,8];
    wal.append_entry(WriteAheadLogEntry::new(1, 2,data2)).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize*2 as usize) as u64+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut log_reader=wal.record_entry_iterator().unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test((WriteAheadLog::block_size() as usize - 30 as usize) as u32);
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=vec![5,6,7,8];
    assert_eq!(*data_read.data(),data);
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_write_0_size_entry() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    assert!(wal.append_entry(WriteAheadLogEntry::new(1, 1, vec![])).err().is_some());
}

#[test]
fn test_create_and_reset_log() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    let data1: Vec<u8> = create_vector_data_for_test((10 as usize) as u32);
    wal.append_entry(WriteAheadLogEntry::new(1, 1, data1)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(15);
    wal.append_entry(WriteAheadLogEntry::new(1, 2, data2)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(20);
    wal.append_entry(WriteAheadLogEntry::new(1, 3, data2)).unwrap();
    wal.seek_and_clear_after(1,2).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(20);
    wal.append_entry(WriteAheadLogEntry::new(1, 3, data2)).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize) as u64+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut log_reader=wal.record_entry_iterator().unwrap();
    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test((10 as usize) as u32);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(15);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(20);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_reset_log_01() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    wal.append_entry(WriteAheadLogEntry::new(1, 1, create_vector_data_for_test(10))).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 2, create_vector_data_for_test_01(15))).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 3, create_vector_data_for_test_01(20))).unwrap();
    wal.seek_and_clear_after(1,1).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 3, create_vector_data_for_test_02(25))).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize) as u64+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut log_reader=wal.record_entry_iterator().unwrap();

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test((10 as usize) as u32));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_02(25));


    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_flush_and_reopen() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    wal.append_entry(WriteAheadLogEntry::new(1, 1, create_vector_data_for_test(10))).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 2, create_vector_data_for_test_01(15))).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 3, create_vector_data_for_test_01(20))).unwrap();
    wal.seek_and_clear_after(1,1).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 3, create_vector_data_for_test_02(25))).unwrap();
    wal.flush().unwrap();

    let path_str= wal.path().clone().into_os_string().into_string().unwrap();
    let mut wal = WriteAheadLog::from_path(&*path_str).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 4, create_vector_data_for_test_03(30))).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize) as u64+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut log_reader=wal.record_entry_iterator().unwrap();

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test((10 as usize) as u32));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_02(25));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_03(30));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_flush_multiple_times() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    wal.append_entry(WriteAheadLogEntry::new(1, 1, create_vector_data_for_test(10))).unwrap();
    wal.flush().unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 2, create_vector_data_for_test_01(15))).unwrap();
    wal.flush().unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 3, create_vector_data_for_test_02(20))).unwrap();
    wal.flush().unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 4, create_vector_data_for_test_03(25))).unwrap();
    wal.flush().unwrap();

    let path_str= wal.path().clone().into_os_string().into_string().unwrap();
    let mut wal = WriteAheadLog::from_path(&*path_str).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 5, create_vector_data_for_test_03(30))).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize) as u64+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut log_reader=wal.record_entry_iterator().unwrap();

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test(10));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_01(15));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_02(20));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_03(25));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_03(30));


    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_flush_and_reopen_block_boundary() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    wal.append_entry(WriteAheadLogEntry::new(1, 1, create_vector_data_for_test((WriteAheadLog::block_size() - 15) as u32))).unwrap();
    wal.flush().unwrap();

    let path_str= wal.path().clone().into_os_string().into_string().unwrap();
    let mut wal = WriteAheadLog::from_path(&*path_str).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 4, create_vector_data_for_test_03(30))).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize*2) as u64+1);
    let mut log_reader=wal.record_entry_iterator().unwrap();

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test((WriteAheadLog::block_size() - 15) as u32));


    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_03(30));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}


#[test]
fn test_create_and_flush_and_reopen_block_boundary_2_blocks() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    wal.append_entry(WriteAheadLogEntry::new(1, 1, create_vector_data_for_test((WriteAheadLog::block_size() - 15) as u32))).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 1, create_vector_data_for_test_01((WriteAheadLog::block_size() - 15) as u32))).unwrap();
    wal.flush().unwrap();

    let path_str= wal.path().clone().into_os_string().into_string().unwrap();
    let mut wal = WriteAheadLog::from_path(&*path_str).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 4, create_vector_data_for_test_03(30))).unwrap();
    wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize*3) as u64+1);
    let file = OpenOptions::new().read(true).open(&wal.path()).unwrap();
    let mut log_reader=wal.record_entry_iterator().unwrap();

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test((WriteAheadLog::block_size() - 15) as u32));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_01((WriteAheadLog::block_size() - 15) as u32));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test_03(30));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_read_without_flush() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    let data1: Vec<u8> = create_vector_data_for_test(10);
    wal.append_entry(WriteAheadLogEntry::new(1, 1, data1)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(15);
    wal.append_entry(WriteAheadLogEntry::new(1, 2, data2)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(20);
    wal.append_entry(WriteAheadLogEntry::new(1, 3, data2)).unwrap();


    //wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),1);
    let mut log_reader=wal.record_entry_iterator().unwrap();

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test(10);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(15);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(20);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_read_without_flush_multiple_blocks() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    let data1: Vec<u8> = create_vector_data_for_test(WriteAheadLog::block_size() as u32);
    wal.append_entry(WriteAheadLogEntry::new(1, 1, data1)).unwrap();
    wal.flush();
    let data2: Vec<u8> = create_vector_data_for_test_01(15);
    wal.append_entry(WriteAheadLogEntry::new(1, 2, data2)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(20);
    wal.append_entry(WriteAheadLogEntry::new(1, 3, data2)).unwrap();


    //wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize*2) as u64+1);
    let mut log_reader=wal.record_entry_iterator().unwrap();

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test(WriteAheadLog::block_size() as u32);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(15);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(20);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_seek_without_flush() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    let data1: Vec<u8> = create_vector_data_for_test(10);
    wal.append_entry(WriteAheadLogEntry::new(1, 1, data1)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(15);
    wal.append_entry(WriteAheadLogEntry::new(1, 2, data2)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(20);
    wal.append_entry(WriteAheadLogEntry::new(1, 3, data2)).unwrap();


    //wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),1);
    let mut log_reader=wal.record_entry_iterator().unwrap();
    log_reader.seek(1,2);
    let opt_entry=log_reader.next();

    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(20);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_seek_without_flush_multiple_blocks() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    let data1: Vec<u8> = create_vector_data_for_test(WriteAheadLog::block_size() as u32);
    wal.append_entry(WriteAheadLogEntry::new(1, 1, data1)).unwrap();
    wal.flush();
    let data2: Vec<u8> = create_vector_data_for_test_01(15);
    wal.append_entry(WriteAheadLogEntry::new(1, 2, data2)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(20);
    wal.append_entry(WriteAheadLogEntry::new(1, 3, data2)).unwrap();


    //wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize*2) as u64+1);
    let mut log_reader=wal.record_entry_iterator().unwrap();
    log_reader.seek(1,1);


    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(15);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(20);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}

#[test]
fn test_create_and_seek_without_flush_multiple_blocks_01() {
    let dir = create_test_dir();

    let mut wal = WriteAheadLog::new(dir.as_str()).unwrap();

    wal.append_entry(WriteAheadLogEntry::new(1, 1, create_vector_data_for_test(WriteAheadLog::block_size() as u32))).unwrap();
    wal.append_entry(WriteAheadLogEntry::new(1, 2, create_vector_data_for_test(WriteAheadLog::block_size() as u32))).unwrap();
    wal.flush();
    let data2: Vec<u8> = create_vector_data_for_test_01(15);
    wal.append_entry(WriteAheadLogEntry::new(1, 3, data2)).unwrap();
    let data2: Vec<u8> = create_vector_data_for_test_01(20);
    wal.append_entry(WriteAheadLogEntry::new(1, 4, data2)).unwrap();


    //wal.flush().unwrap();

    let file_metadata = metadata(&wal.path());
    assert_eq!(file_metadata.unwrap().len(),(WriteAheadLog::block_size()as usize*3) as u64+1);
    let mut log_reader=wal.record_entry_iterator().unwrap();
    log_reader.seek(1,1);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    assert_eq!(*data_read.data(),create_vector_data_for_test(WriteAheadLog::block_size() as u32));

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(15);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_some());
    let data_read=opt_entry.unwrap();
    let data: Vec<u8>=create_vector_data_for_test_01(20);
    assert_eq!(*data_read.data(),data);

    let opt_entry=log_reader.next();
    assert!(opt_entry.is_none());
}