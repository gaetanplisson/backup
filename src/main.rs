use async_compression::tokio::write::GzipEncoder;
use bytes::Bytes;
use futures::future::join_all;
use futures::io::AsyncReadExt;
use futures::stream::StreamExt;
use futures::Future;
use futures::Stream;
use futures::TryFutureExt;
use futures::TryStreamExt;
use opendal::raw::HttpClient;
use opendal::services::Ftp;
use opendal::services::Webdav;
use opendal::{Entry, EntryMode, Operator};
use rayon::{prelude::*, vec};
use reqwest;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio;
use tokio::fs::File;
use tokio::sync::Mutex;
use tokio::task;
use tokio::task::JoinHandle;
use tokio_tar::{Builder, Header};
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::compat::Compat;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio::io::AsyncReadExt as tokio_async_read_ext;
use tokio::io::SeekFrom;

fn archive_builder(file: File) -> Result<Builder<GzipEncoder<File>>, io::Error> {
    let enc: GzipEncoder<File> = GzipEncoder::new(file);
    let mut tar: Builder<GzipEncoder<File>> = Builder::new(enc);
    Ok(tar)
}

async fn send_ftp(op: Operator, mut from: File, to: &str) -> Result<u64, io::Error> {
    let mut writer: opendal::Writer = op.writer_with(to).await?;
    from.seek(SeekFrom::Start(0)).await?;
    let compat: Compat<File> = from.compat();
    let res: Result<u64, io::Error> = writer.copy(compat)
        .map_err(|e: opendal::Error| std::io::Error::new(std::io::ErrorKind::Other, e.to_string())).await;
    opendal::Writer::flush(&mut writer).await?;
    opendal::Writer::close(&mut writer).await?;
    res
}

fn connect_ftp() -> Result<Operator, io::Error> {
    let mut builder = opendal::services::Ftp::default();
    builder.endpoint(&"ftp://".to_string()); // archive server
    builder.user(&"".to_string()); // user 
    builder.password(&"".to_string()); // passwd
    builder.root(&"/".to_string());
    let op = Operator::new(builder)?.finish();
    println!("{:?}", op);
    Ok(op)
}

fn connect_webdav() -> Result<Operator, io::Error> {
    let mut builder = Webdav::default();
    let client = reqwest::ClientBuilder::new().danger_accept_invalid_certs(true);
    let http_client = HttpClient::build(client)?;

    builder.endpoint("https://ip/nextcloud/remote.php/dav/files/backup_bot"); // nextcloud ip
    builder.username("backup_bot");
    builder.password(""); // backup bot passwd
    builder.http_client(http_client);

    let op_builder_res = Operator::new(builder);
    let op = match op_builder_res {
        Ok(op_builder) => {
            let op_builder = op_builder;
            op_builder.finish()
        }
        Err(e) => panic!("Error: {}", e),
    };
    Ok(op)
}

async fn download_files_to_archive(
    op: Operator,
    path: &str,
    archive: Builder<GzipEncoder<File>>,
) -> Result<File, io::Error> {
    let entries_lister: opendal::Lister = op.lister_with(path).recursive(true).await?;
    let op_mutex = Arc::new(Mutex::new(op));

    let entries_tasks: Vec<JoinHandle<Result<(), io::Error>>> = Vec::new();
    let entries_tasks_arc = Arc::new(Mutex::new(entries_tasks));

    let archive_mutex: Arc<Mutex<Builder<GzipEncoder<File>>>> = Arc::new(Mutex::new(archive));

    entries_lister
        .for_each_concurrent(None, |entry_res| async {
            if let Ok(entry) = entry_res {
                let op_clone = Arc::clone(&op_mutex);
                let archive_clone = Arc::clone(&archive_mutex);
                let entry_task: JoinHandle<Result<(), io::Error>> = task::spawn(async move {
                    let meta = entry.metadata();
                    match meta.mode() {
                        EntryMode::FILE => {
                            let op = op_clone.lock().await;
                            let reader = op.reader(entry.path()).await?;
                            let mut header = Header::new_gnu();
                            header.set_path(entry.path())?;
                            header.set_size(meta.content_length());
                            header.set_cksum();
                            let mut archive = archive_clone.lock().await;

                            (*archive).append(&header, reader).await?;
                            Ok(())
                        }
                        EntryMode::DIR => Ok(()),
                        _ => Ok(()),
                    }
                });
                let mut entries_tasks_guard = entries_tasks_arc.lock().await;
                entries_tasks_guard.push(entry_task);
            }
        })
        .await;

    let mut entries_tasks_mutex = entries_tasks_arc.lock().await;
    let entries_tasks: std::vec::Drain<JoinHandle<_>> = entries_tasks_mutex.drain(..);
    let vec_res: Vec<Result<Result<(), io::Error>, task::JoinError>> =
        join_all(entries_tasks).await;
    for task in vec_res {
        if let Err(err) = task? {
            return Err(err);
        }
    }
    let underlying_archive = Arc::try_unwrap(archive_mutex);

    match underlying_archive {
        Ok(archive) => {
            let builder_encoder_file = archive.into_inner();
            let gzip = builder_encoder_file.into_inner().await?;
            let mut file: File = gzip.into_inner();
            file.flush().await?;

            Ok(file)
        }
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "Error")),
    }
}

async fn backup() -> Result<(), io::Error> {
    let local_file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open("./test.tar.gz")
        .await?;

    let opdav = connect_webdav()?;
    let tar = archive_builder(local_file)?;
    let opftp = connect_ftp()?;

    let archive_file = download_files_to_archive(opdav, "/Photos/", tar).await?;

    let res = send_ftp(opftp, archive_file, "./home/backups/test.tar.gz").await?;
    //send_ftp(opftp, archive_file, "./home/").await?;
    Ok(())
}


async fn test_transport(to: &str) -> () {
    let op = connect_ftp().unwrap();
    let lister = op.lister(to).await.unwrap();
    let vec: Vec<Entry> = Vec::new();
    let vec_mut = Arc::new(Mutex::new(vec));
    lister
        .try_for_each_concurrent(None, |e| async {
            let vec_clone = Arc::clone(&vec_mut);
            let mut vec_mut = vec_clone.lock().await;
            vec_mut.push(e);
            Ok(())
        })
        .await
        .unwrap();
    let vec_clone = Arc::clone(&vec_mut);
    let vec_mut = vec_clone.lock().await;
    println!("{:?}", *vec_mut);
}

#[tokio::main]
async fn main() {
    // test_transport("./home/backups/").await;
    let res = backup().await;
    match res {
        Ok(_) => println!("Backup finished"),
        Err(e) => println!("Error: {}", e),
    }

    println!("Hello, world!");
}
