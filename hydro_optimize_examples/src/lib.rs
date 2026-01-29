stageleft::stageleft_no_entry_crate!();

pub mod network_calibrator;
pub mod simple_kv_bench;
pub mod lock_server;
// pub mod lobsters;
// pub mod web_submit;

#[cfg(test)]
mod test_init {
    #[ctor::ctor]
    fn init() {
        hydro_lang::deploy::init_test();
    }
}
