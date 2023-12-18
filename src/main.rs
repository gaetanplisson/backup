use std::fs::File;
use flate2::Compression;
use flate2::write::GzEncoder;
use tar::Builder;
use std::env;
use opendal::services::Webdav;
use opendal::Operator;



fn create_tar_gz() -> std::io::Result<()> {
    let tar_gz = File::create("test.tar.gz")?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);
    tar.append_dir_all("test", "test")?;
    tar.finish()?;
    Ok(())
}

async fn connect_webdav() -> std::io::Result<()> {
    let mut builder = Webdav::default();

    builder.endpoint("https://10.0.4.90/nextcloud/remote.php/dav/files/backup_bot");
    builder.username("backup_bot");
    builder.password("42PJdt2SZwY7fC");

    let op: Operator = Operator::new(builder)?.finish();
    let mut entries = op.list("./").await?;
    Ok(())
    
}

fn main() {
    const NEXTCLOUD_PATH_TO_BACKUP: &str = "/var/www/nextcloud";
    connect_webdav();
    println!("Hello, world!");
    loop{}
}
