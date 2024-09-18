pub fn print_features() {
    if cfg!(feature = "benchmark") {
        log::debug!("benchmark enabled");
    } else {
        log::debug!("--- benchmark disabled ---");
    }
}
