use env_logger::Builder;
use std::io::Write;

pub fn init_log() {
    let mut builder = Builder::from_default_env();
    match builder
        .format_timestamp_secs()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{}] [{}:{}] {}",
                record.level(),
                record.file().unwrap(),
                record.line().unwrap(),
                record.args()
            )
        })
        .try_init()
    {
        Ok(_) => (),
        Err(_) => (),
    }
}
