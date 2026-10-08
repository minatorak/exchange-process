//! exchange-process process entry point. Thin: the runtime layer owns lifecycle.

mod api;
mod application;
#[cfg(test)]
mod boundary;
mod domain;
mod infrastructure;
mod runtime;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    if let Err(err) = rt.block_on(crate::runtime::run::run()) {
        eprintln!("exchange-process: {err:#}");
        std::process::exit(1);
    }
}
