use std::cell::RefCell;
use std::collections::HashMap;

use hydro_lang::compile::builder::FlowBuilder;
use hydro_lang::compile::built::BuiltFlow;
use hydro_lang::compile::ir::{
    BoundKind, CollectionKind, DebugType, HydroIrMetadata, HydroNode, HydroRoot,
    KeyedSingletonBoundKind, StreamOrder, StreamRetry, deep_clone, traverse_dfir,
};
use hydro_lang::deploy::HydroDeploy;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Cluster, LocationKey};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use serde::{Deserialize, Serialize};
use syn::parse_quote;
use syn::visit_mut::{self, VisitMut};

use crate::decoupler::{self, DecoupleDecision};
use crate::partitioner::Partitioner;

#[derive(Clone, Serialize, Deserialize)]
pub enum Rewrite {
    Decouple {
        decision: DecoupleDecision,
        orig_location: LocationId,
    },
    Partition(Partitioner),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RewriteMetadata {
    pub node: LocationId,
    pub num_nodes: usize,
    pub rewrite: Rewrite,
}

pub type Rewrites = Vec<RewriteMetadata>;

/// Replays the rewrites in order.
/// Returns Vec(Cluster, number of nodes) for each created cluster and a new FlowBuilder
pub fn replay<'a>(
    rewrites: Rewrites,
    built: BuiltFlow<'a>,
) -> (Vec<(Cluster<'a, ()>, usize)>, FlowBuilder<'a>) {
    let mut all_new_clusters = vec![];

    let mut ir = deep_clone(built.ir());
    let mut builder = FlowBuilder::from_built(&built);

    // Apply decoupling/partitioning in order
    for rewrite_metadata in rewrites {
        match rewrite_metadata.rewrite {
            Rewrite::Decouple {
                decision,
                orig_location,
            } => {
                let new_clusters =
                    decoupler::decouple(&mut ir, decision, &orig_location, &mut builder);
                for cluster in new_clusters {
                    all_new_clusters.push((cluster, rewrite_metadata.num_nodes));
                }
            }
            Rewrite::Partition(_partitioner) => {
                panic!("Partitioning is not yet replayable");
            }
        }
    }

    builder.replace_ir(ir);

    (all_new_clusters, builder)
}

/// Replace CLUSTER_SELF_ID with the ID of the original node the partition is assigned to
#[derive(Copy, Clone)]
pub enum ClusterSelfIdReplace {
    Decouple {
        orig_cluster_id: LocationKey,
        decoupled_cluster_id: LocationKey,
    },
    Partition {
        num_partitions: usize,
        partitioned_cluster_id: LocationKey,
        op_id: usize,
    },
}

impl VisitMut for ClusterSelfIdReplace {
    fn visit_expr_mut(&mut self, expr: &mut syn::Expr) {
        if let syn::Expr::Path(path_expr) = expr {
            for segment in path_expr.path.segments.iter_mut() {
                let ident = segment.ident.to_string();

                match self {
                    ClusterSelfIdReplace::Decouple {
                        orig_cluster_id,
                        decoupled_cluster_id,
                    } => {
                        let prefix = format!("__hydro_lang_cluster_self_id_{}", orig_cluster_id);
                        if ident.starts_with(&prefix) {
                            segment.ident = syn::Ident::new(
                                &format!("__hydro_lang_cluster_self_id_{}", decoupled_cluster_id),
                                segment.ident.span(),
                            );
                            println!("Decoupling: Replaced CLUSTER_SELF_ID");
                            return;
                        }
                    }
                    ClusterSelfIdReplace::Partition {
                        num_partitions,
                        partitioned_cluster_id,
                        op_id,
                    } => {
                        let prefix =
                            format!("__hydro_lang_cluster_self_id_{}", partitioned_cluster_id);
                        if ident.starts_with(&prefix) {
                            let expr_content = std::mem::replace(expr, syn::Expr::PLACEHOLDER);
                            *expr = syn::parse_quote!({
                                #expr_content / #num_partitions as u32
                            });
                            println!("Partitioning: Replaced CLUSTER_SELF_ID for node {}", op_id);
                            return;
                        }
                    }
                }
            }
        }
        visit_mut::visit_expr_mut(self, expr);
    }
}

/// Converts input metadata to IDs, filtering by location if provided
pub fn relevant_inputs(
    input_metadatas: Vec<&HydroIrMetadata>,
    location: Option<&LocationKey>,
) -> Vec<usize> {
    input_metadatas
        .iter()
        .filter_map(|input_metadata| {
            if let Some(location) = location
                && input_metadata.location_id.root().key() != *location
            {
                None
            } else {
                Some(input_metadata.op.id.unwrap())
            }
        })
        .collect()
}

/// Creates a mapping from op_id to its input op_ids, filtered by location if provided
pub fn op_id_to_inputs(
    ir: &mut [HydroRoot],
    location: Option<&LocationKey>,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> HashMap<usize, Vec<usize>> {
    let mapping = RefCell::new(HashMap::new());

    traverse_dfir::<HydroDeploy>(
        ir,
        |leaf, op_id| {
            let relevant_input_ids = relevant_inputs(vec![leaf.input_metadata()], location);
            mapping.borrow_mut().insert(*op_id, relevant_input_ids);
        },
        |node, op_id| {
            let input_ids = match node {
                HydroNode::CycleSource { .. } => {
                    // For CycleSource, its input is its CycleSink's input. Note: assumes the CycleSink is on the same cluster
                    vec![*cycle_source_to_sink_input.get(op_id).unwrap()]
                }
                HydroNode::Tee { inner, .. } => {
                    vec![inner.0.borrow().op_metadata().id.unwrap()]
                }
                _ => relevant_inputs(node.input_metadata(), location),
            };
            mapping.borrow_mut().insert(*op_id, input_ids);
        },
    );

    mapping.take()
}

/// Creates a mapping from op_id to the other (if any) op_id that outputs to the same node.
pub fn op_id_to_partner(ir: &mut [HydroRoot]) -> HashMap<usize, usize> {
    let mut output = HashMap::new();

    traverse_dfir::<HydroDeploy>(ir, |_, _| {}, |node, _op_id| {
        let input_metadatas = node.input_metadata();
        if input_metadatas.len() == 2 {
            let dad_op_id = input_metadatas[0].op.id.unwrap();
            let mom_op_id = input_metadatas[1].op.id.unwrap();
            output.insert(dad_op_id, mom_op_id);
            output.insert(mom_op_id, dad_op_id);
        }
        assert!(input_metadatas.len() > 2, "Logic needs to be updated to handle nodes with more than 2 inputs");
    });
    output
}

pub fn tee_to_inner_id(ir: &mut [HydroRoot]) -> HashMap<usize, usize> {
    let mut mapping = HashMap::new();

    traverse_dfir::<HydroDeploy>(
        ir,
        |_, _| {},
        |node, op_id| {
            if let HydroNode::Tee { inner, .. } = node {
                mapping.insert(*op_id, inner.0.borrow().op_metadata().id.unwrap());
            }
        },
    );

    mapping
}

#[derive(Clone, PartialEq, Eq)]
pub enum NetworkType {
    Recv,
    Send,
    SendRecv,
}

pub fn get_network_type(node: &HydroNode, location: &LocationKey) -> Option<NetworkType> {
    let mut is_to_us = false;
    let mut is_from_us = false;

    if let HydroNode::Network { input, .. } = node {
        if input.metadata().location_id.root().key() == *location {
            is_from_us = true;
        }
        if node.metadata().location_id.root().key() == *location {
            is_to_us = true;
        }

        return if is_from_us && is_to_us {
            Some(NetworkType::SendRecv)
        } else if is_from_us {
            Some(NetworkType::Send)
        } else if is_to_us {
            Some(NetworkType::Recv)
        } else {
            None
        };
    }
    None
}

fn get_this_crate() -> TokenStream {
    let hydro_lang_crate = proc_macro_crate::crate_name("hydro_lang")
        .expect("hydro_lang should be present in `Cargo.toml`");
    match hydro_lang_crate {
        proc_macro_crate::FoundCrate::Itself => quote! { hydro_lang },
        proc_macro_crate::FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, Span::call_site());
            quote! { #ident }
        }
    }
}

pub fn serialize_bincode_with_type(is_demux: bool, t_type: &syn::Type) -> syn::Expr {
    let root = get_this_crate();

    if is_demux {
        parse_quote! {
            ::#root::runtime_support::stageleft::runtime_support::fn1_type_hint::<(#root::location::MemberId<_>, #t_type), _>(
                |(id, data)| {
                    (id.into_tagless(), #root::runtime_support::bincode::serialize(&data).unwrap().into())
                }
            )
        }
    } else {
        parse_quote! {
            ::#root::runtime_support::stageleft::runtime_support::fn1_type_hint::<#t_type, _>(
                |data| {
                    #root::runtime_support::bincode::serialize(&data).unwrap().into()
                }
            )
        }
    }
}

pub fn deserialize_bincode_with_type(tagged: Option<&syn::Type>, t_type: &syn::Type) -> syn::Expr {
    let root = get_this_crate();

    if let Some(c_type) = tagged {
        parse_quote! {
            |res| {
                let (id, b) = res.unwrap();
                (#root::location::MemberId::<#c_type>::from_tagless(id as #root::__staged::location::TaglessMemberId), #root::runtime_support::bincode::deserialize::<#t_type>(&b).unwrap())
            }
        }
    } else {
        parse_quote! {
            |res| {
                #root::runtime_support::bincode::deserialize::<#t_type>(&res.unwrap()).unwrap()
            }
        }
    }
}

pub fn collection_kind_to_debug_type(collection_kind: &CollectionKind) -> DebugType {
    match collection_kind {
        CollectionKind::Stream { element_type, .. }
        | CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => DebugType::from(*element_type.clone().0),
        CollectionKind::KeyedStream {
            key_type,
            value_type,
            ..
        }
        | CollectionKind::KeyedSingleton {
            key_type,
            value_type,
            ..
        } => {
            let original_key_type = *key_type.clone().0;
            let original_value_type = *value_type.clone().0;
            let new_type: syn::Type = syn::parse_quote! {
                (#original_key_type, #original_value_type)
            };
            DebugType::from(new_type)
        }
    }
}

pub fn prepend_member_id_to_collection_kind(collection_kind: &CollectionKind) -> CollectionKind {
    let member_id_syn_type: syn::Type = syn::parse_quote! { ::hydro_lang::location::MemberId<()> };
    let member_id_debug_type = DebugType::from(member_id_syn_type);
    match collection_kind {
        CollectionKind::Singleton { element_type, .. }
        | CollectionKind::Optional { element_type, .. } => CollectionKind::KeyedSingleton {
            bound: KeyedSingletonBoundKind::Unbounded,
            key_type: member_id_debug_type,
            value_type: element_type.clone(),
        },
        CollectionKind::Stream { .. }
        | CollectionKind::KeyedStream { .. }
        | CollectionKind::KeyedSingleton { .. } => CollectionKind::KeyedStream {
            bound: BoundKind::Unbounded,
            value_order: StreamOrder::NoOrder,
            value_retry: StreamRetry::ExactlyOnce,
            key_type: member_id_debug_type,
            value_type: collection_kind_to_debug_type(collection_kind),
        },
    }
}
