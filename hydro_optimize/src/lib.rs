#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod debug;
pub mod decouple_analysis;
pub mod decoupler;
pub mod deploy;
pub mod deploy_and_analyze;
pub mod parse_results;
pub mod partition_node_analysis;
pub mod partition_syn_analysis;
pub mod partitioner;
pub mod repair;
pub mod rewrites;

#[doc(hidden)]
#[cfg(doctest)]
mod docs {
    include_mdtests::include_mdtests!("docs/**/*.md*");
}
