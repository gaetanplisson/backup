use async_compression::futures::write::GzipEncoder;
use futures::Stream;
use opendal::services::Webdav;
use opendal::{Operator, Entry};
use opendal::raw::HttpClient;
use reqwest;
use async_tar::Builder;
use async_fs::File;
use futures::stream::StreamExt;
use tokio;
use tokio::task;
use tokio::task::JoinHandle;
use futures::future::{join_all, Join};
use rayon::prelude::*;
use std::sync::{Arc,Mutex};

async fn archive_builder() -> Option<Builder<GzipEncoder<File>>> {
    let tar_gz = match File::create("test.tar.gz").await{
        Ok(archive) => archive,
        Err(_)=> return None
    };
    let enc = GzipEncoder::new(tar_gz);
    let mut tar: Builder<GzipEncoder<File>> = Builder::new(enc);
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
    
    let entries_lister: opendal::Lister = op.lister_with(path)
        .recursive(true).await.unwrap(); 
    let op_arc = Arc::new(op);
    let entries_tasks: Vec<JoinHandle<()>> = Vec::new();
    let entries_tasks_arc = Arc::new(Mutex::new(entries_tasks));
    entries_lister.for_each_concurrent(None, |entry_res| async {
    let opclone = Arc::clone(&op_arc);
        if let Ok(entry) = entry_res {

            let entry_task = task::spawn( async move{
                let reader = (*opclone).reader(entry.path()).await.unwrap();
            });
            let mut entries_tasks_mutex = entries_tasks_arc.lock().unwrap();
            (*entries_tasks_mutex).push(entry_task);
            
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
