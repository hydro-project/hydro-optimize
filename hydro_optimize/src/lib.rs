#[cfg(stageleft_runtime)]
// hydro_lang::setup!();
// TODO: Temporary stageleft hack from Shadaj to get tests and dependent repos working
#[allow(
    ambiguous_glob_reexports,
    mismatched_lifetime_syntaxes,
    unexpected_cfgs,
    unfulfilled_lint_expectations,
    unused,
    clippy::suspicious_else_formatting,
    clippy::type_complexity,
    reason = "generated code"
)]
pub mod __staged {
    #[cfg(any(feature = "stageleft_macro_entrypoint", stageleft_trybuild))]
    include!(concat!(
        env!("OUT_DIR"),
        stageleft::PATH_SEPARATOR!(),
        "lib_pub.rs"
    ));

    // #[cfg(test)]
    // include!(concat!(
    //     env!("OUT_DIR"),
    //     stageleft::PATH_SEPARATOR!(),
    //     "staged_deps.rs"
    // ));
}

#[cfg(stageleft_runtime)]
#[cfg(test)]
mod test_init {
    #[ctor::ctor]
    fn init() {
        hydro_lang::compile::init_test();
    }
}

#[allow(dead_code)]
pub mod debug;
#[allow(dead_code)]
pub mod decouple_analysis;
#[allow(dead_code)]
pub mod decoupler;
pub mod deploy;
pub mod deploy_and_analyze;
#[allow(dead_code)]
pub mod greedy_decouple_analysis;
pub mod parse_results;
#[allow(dead_code)]
pub mod partial_partitioner;
#[allow(dead_code)]
pub mod partition_node_analysis;
#[allow(dead_code)]
pub mod partition_syn_analysis;
#[allow(dead_code)]
pub mod partitioner;
pub mod reduce_pushdown;
pub mod reduce_pushdown_analysis;
pub mod repair;
pub mod rewrites;

#[doc(hidden)]
#[cfg(doctest)]
mod docs {
    include_mdtests::include_mdtests!("docs/**/*.md*");
}
