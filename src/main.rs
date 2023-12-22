use async_compression::tokio::write::GzipEncoder;
use opendal::services::Webdav;
use opendal::{Operator, Entry};
use opendal::raw::HttpClient;
use reqwest;
use tokio_tar::Builder;
use futures::stream::StreamExt;
use tokio;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::fs::File;
use rayon::prelude::*;
use std::sync::{Arc,Mutex};

async fn archive_builder() -> Result<Builder<GzipEncoder<File>>, std::io::Error>{
    match File::create("test.tar.gz").await{
        Ok(archive) => {
            let tar_gz = archive;
            let enc = GzipEncoder::new(tar_gz);
            let mut tar: Builder<GzipEncoder<File>> = Builder::new(enc);
            Ok(tar)
        },
        Err(_)=> {Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"Error creating archive"))}
    }
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



async fn download_files_to_archive (op: Operator, path: &str, archive : &mut Builder<GzipEncoder<File>> )-> () {
    
    let entries_lister: opendal::Lister = op.lister_with(path)
        .recursive(true).await.unwrap(); 
    let op_arc = Arc::new(op);
    let entries_tasks: Vec<JoinHandle<()>> = Vec::new();
    let entries_tasks_arc = Arc::new(Mutex::new(entries_tasks));
    let archive_mutex = Arc::new(Mutex::new(archive));
    
    entries_lister.for_each_concurrent(None, |entry_res| async {
        if let Ok(entry) = entry_res {
            let opclone = Arc::clone(&op_arc);
            let entry_task = task::spawn( async move{
                let reader = (*opclone).reader(entry.path()).await.unwrap();
                let mut archive_guard = archive_mutex.lock().unwrap();
                (*archive_guard).append_file(entry.path(), reader).await.unwrap();

            });
            let mut entries_tasks_guard = entries_tasks_arc.lock().unwrap();
            (*entries_tasks_guard).push(entry_task);
            
        }
    }).await;
    let mut entries_tasks_mutex = entries_tasks_arc.lock().unwrap();
    let entries_tasks: std::vec::Drain<JoinHandle<_>> = entries_tasks_mutex.drain(..);
    join_all(entries_tasks).await;
    //     tokio::join!(task);
    // }
    // while let Some(entry_res) = entries_lister.next().await {
        
    //     // let entry_task = task::spawn(async move{
    //     //     if let Ok(entry) = entry_res {
    //     //         let mut reader = (opclone).reader(entry.path()).await.unwrap();
    //     //     }
    //     // });

    //     // entries_tasks.push(entry_task);
    // }
    // join_all(entries_tasks).await;
    ()
}

#[tokio::main]
async fn main() {
    let op = connect_webdav();
   // let mut tar = archive_builder().unwrap();
    println!("Hello, world!");
}
