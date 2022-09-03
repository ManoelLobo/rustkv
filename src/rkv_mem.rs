use librustkv::RustKV;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
  rkv_mem.exe <FILE> get <key>
  rkv_mem.exe <FILE> delete <key>
  rkv_mem.exe <FILE> insert <key> <value>
  rkv_mem.exe <FILE> update <key> <value>
";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
  rkv_mem <FILE> get <key>
  rkv_mem <FILE> delete <key>
  rkv_mem <FILE> insert <key> <value>
  rkv_mem <FILE> update <key> <value>
";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let filename = args.get(1).expect(USAGE);
    let command = args.get(2).expect(USAGE).as_ref();
    let key = args.get(3).expect(USAGE).as_ref();
    let value_if_given = args.get(4);

    let path = std::path::Path::new(&filename);
    let mut store = RustKV::open(path).expect("Failed to open store file");
    store.load().expect("Failed to load store contents");

    match command {
        "get" => match store.get(key).unwrap() {
            // TODO: Display the key as String again (as_ref converted to &[u8])
            None => println!("Key {:?} not found", key),
            Some(value) => println!("{value:?}"),
        },

        "delete" => store.delete(key).unwrap(),

        "insert" => {
            let value = value_if_given.expect(USAGE).as_ref();
            store.insert(key, value).unwrap();
        }

        "update" => {
            let value = value_if_given.expect(USAGE).as_ref();
            store.update(key, value).unwrap();
        }

        _ => eprintln!("{}", USAGE),
    }
}
