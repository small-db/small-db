use env_logger::Builder;
use std::io::Write;

pub fn init_log() {
    let mut builder = Builder::from_default_env();
    builder
        .format_timestamp_secs()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} - {}] [{}:{}] {}",
                record.level(),
                record.target(),
                record.file().unwrap(),
                record.line().unwrap(),
                record.args()
            )
        })
        .init();
}
