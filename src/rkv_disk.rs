use std::collections::HashMap;

use librustkv::RustKV;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
  rkv_disk.exe <FILE> get <key>
  rkv_disk.exe <FILE> delete <key>
  rkv_disk.exe <FILE> insert <key> <value>
  rkv_disk.exe <FILE> update <key> <value>
";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
  rkv_disk <FILE> get <key>
  rkv_disk <FILE> delete <key>
  rkv_disk <FILE> insert <key> <value>
  rkv_disk <FILE> update <key> <value>
";

type ByteStr = [u8];
type ByteString = Vec<u8>;

fn store_index_on_disk(store: &mut RustKV, index_key: &ByteStr) {
    store.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&store.index).unwrap();
    store.index = std::collections::HashMap::new();
    store.insert(index_key, &index_as_bytes).unwrap();
}

fn main() {
    const INDEX_KEY: &ByteStr = b"+index";

    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(USAGE);
    let command = args.get(2).expect(USAGE).as_ref();
    let key = args.get(3).expect(USAGE).as_ref();
    let value_if_given = args.get(4);

    let path = std::path::Path::new(&fname);
    let mut store = RustKV::open(path).expect("Failed to open store file");
    store.load().expect("Failed to load store contents");

    match command {
        "get" => {
            let index_as_bytes = store.get(INDEX_KEY).unwrap().unwrap();

            let index_decoded = bincode::deserialize(&index_as_bytes);

            let index: HashMap<ByteString, u64> = index_decoded.unwrap();

            match index.get(key) {
                None => eprintln!("{:?} not found", key),
                Some(&i) => {
                    let kv = store.get_at_position(i).unwrap();
                    println!("{:?}", kv.value)
                }
            }
        }

        "delete" => store.delete(key).unwrap(),

        "insert" => {
            let value = value_if_given.expect(USAGE).as_ref();
            store.insert(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        }

        "update" => {
            let value = value_if_given.expect(USAGE).as_ref();
            store.update(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        }
        _ => eprintln!("{}", &USAGE),
    }
}
