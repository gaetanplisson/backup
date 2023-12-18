use std::fs::File;
use flate2::Compression;
use flate2::write::GzEncoder;
use tar::Builder;
use opendal::services::Webdav;
use opendal::Operator;
use opendal::raw::HttpClient;
use tokio;
use reqwest;



fn create_tar_gz() -> std::io::Result<()> {
    let tar_gz = File::create("test.tar.gz")?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);
    tar.append_dir_all("test", "test")?;
    tar.finish()?;
    Ok(())
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

#[tokio::main]
async fn main() {
    let mut op = connect_webdav();
    println!("Hello, world!");
}
