use std::fs::File;
use flate2::Compression;
use flate2::write::GzEncoder;
use futures::future::ErrInto;
use opendal::raw::oio::Stream;
use tar::Builder;
use opendal::services::Webdav;
use opendal::{Operator, Entry};
use opendal::raw::HttpClient;
use tokio;
use reqwest;
use futures::stream::StreamExt;
use tokio::io::AsyncReadExt;



fn archive_builder() -> Option<Builder<GzEncoder<File>>> {
    let tar_gz = match File::create("test.tar.gz"){
        Ok(archive) => archive,
        Err(_)=> return None
    };
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar: Builder<GzEncoder<File>> = Builder::new(enc);
    //tar.append_dir_all("test", "test")?;
    //tar.finish()?;
    Some(tar)
}

// async fn connect_webdav() -> std::io::Result<Vec<opendal::Entry>> {
fn connect_webdav() -> Operator {
    let mut builder = Webdav::default();
    let client = reqwest::ClientBuilder::new()
        .danger_accept_invalid_certs(true);
    let http_client = HttpClient::build(client).unwrap();
    
    builder.endpoint("https://10.0.4.90/nextcloud/remote.php/dav/files/backup_bot");
    builder.username("backup_bot");
    builder.password("42PJdt2SZwY7fC");
    builder.http_client(http_client);

    let op_builder_res = Operator::new(builder);
    let op = match op_builder_res {
        Ok(op_builder) => {
            let op_builder = op_builder;
            op_builder.finish()
        },
        Err(e) => panic!("Error: {}", e),
    };
    op
}

async fn download_files (op: Operator, path: &str)-> () {
    
    let mut entries_lister = op.lister_with(path)
        .recursive(true).await.unwrap(); 

    while let Some(entry) = entries_lister.next().await {
        let mut reader = match entry {
            Ok(entry) => {
                println!("Entry: {:?}", entry);
                let reader = match op.reader(path).await {
                    Ok(r) => r,
                    Err(e) => {println!("Error: {}", e); continue;},
                };
                reader
            },
            Err(e) => {println!("Error: {}", e); continue;},
        };
        let mut buffer: [u8; 1024] = [0 ; 1024]; // 1kb buffer, it may change nothing if more was attributed
        
        let mut tmpFile = File::create("tmpFile").unwrap();

        while reader.read(&mut buffer).await.unwrap() != 0{
            //write buffer into archive
        };
        
    }
    ()
}

#[tokio::main]
async fn main() {
    let mut op = connect_webdav();
    println!("Hello, world!");
}
