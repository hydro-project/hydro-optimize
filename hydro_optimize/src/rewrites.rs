use std::cell::RefCell;
use std::collections::HashMap;

use hydro_lang::compile::ir::{
    BoundKind, CollectionKind, DebugType, HydroIrMetadata, HydroNode, HydroRoot,
    KeyedSingletonBoundKind, SingletonBoundKind, StreamOrder, StreamRetry, traverse_dfir,
};
use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::LocationId;
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use serde::{Deserialize, Serialize};
use syn::parse_quote;
use syn::visit_mut::{self, VisitMut};

use crate::decouple_analysis::PossibleRewrite;

#[derive(Clone, Serialize, Deserialize)]
pub struct Rewrite {
    pub possible_rewrite: PossibleRewrite,
    pub num_partitions: usize,
    pub original_node: LocationId,
    pub cluster_size: usize,
}

/// Replace CLUSTER_SELF_ID with the ID of the original node the partition is assigned to
#[derive(Copy, Clone)]
pub struct ClusterSelfIdReplace {
    pub orig_cluster_id: LocationKey,
    pub new_cluster_id: LocationKey,
    pub num_partitions: usize,
}

impl VisitMut for ClusterSelfIdReplace {
    fn visit_expr_mut(&mut self, expr: &mut syn::Expr) {
        if let syn::Expr::Path(path_expr) = expr {
            for segment in path_expr.path.segments.iter_mut() {
                let ident = segment.ident.to_string();
                let prefix = format!("__hydro_lang_cluster_self_id_{}", self.orig_cluster_id);
                if ident.starts_with(&prefix) {
                    if self.orig_cluster_id != self.new_cluster_id {
                        segment.ident = syn::Ident::new(
                            &format!("__hydro_lang_cluster_self_id_{}", self.new_cluster_id),
                            segment.ident.span(),
                        );
                        println!("Decoupling: Replaced CLUSTER_SELF_ID");
                    }
                    if self.num_partitions > 0 {
                        let num_partitions = self.num_partitions;
                        let expr_content = std::mem::replace(expr, syn::Expr::PLACEHOLDER);
                        *expr = syn::parse_quote!({
                            #expr_content / #num_partitions as u32
                        });
                        println!("Partitioning: Replaced CLUSTER_SELF_ID");
                    }
                    return;
                }
            }
        }
        visit_mut::visit_expr_mut(self, expr);
    }
}

/// Converts input metadata to IDs, filtering by location if provided
pub fn filter_location(
    metadatas: Vec<&HydroIrMetadata>,
    location: Option<&LocationId>,
) -> Vec<usize> {
    metadatas
        .iter()
        .filter_map(|metadata| {
            if let Some(location) = location
                && metadata.location_id.root() != location.root()
            {
                None
            } else {
                Some(metadata.op.id.unwrap())
            }
        })
        .collect()
}

/// Creates a mapping from op_id to its input op_ids, filtered by location if provided
pub fn op_id_to_parents(
    ir: &mut [HydroRoot],
    location: Option<&LocationId>,
    cycle_source_to_sink_input: &HashMap<usize, usize>,
) -> HashMap<usize, Vec<usize>> {
    let mapping = RefCell::new(HashMap::new());

    traverse_dfir(
        ir,
        |leaf, op_id| {
            let relevant_input_ids = filter_location(vec![leaf.input_metadata()], location);
            mapping.borrow_mut().insert(*op_id, relevant_input_ids);
        },
        |node, op_id| {
            let input_ids = match node {
                HydroNode::CycleSource { .. } => {
                    // For CycleSource, its input is its CycleSink's input. Note: assumes the CycleSink is on the same cluster
                    vec![*cycle_source_to_sink_input.get(op_id).unwrap()]
                }
                HydroNode::Tee { inner, .. } | HydroNode::Partition { inner, .. } => {
                    vec![inner.0.borrow().op_metadata().id.unwrap()]
                }
                _ => filter_location(node.input_metadata(), location),
            };
            mapping.borrow_mut().insert(*op_id, input_ids);
        },
    );

    mapping.take()
}

pub fn tee_to_inner_id(ir: &mut [HydroRoot]) -> HashMap<usize, usize> {
    let mut mapping = HashMap::new();

    traverse_dfir(
        ir,
        |_, _| {},
        |node, op_id| {
            if let HydroNode::Tee { inner, .. } | HydroNode::Partition { inner, .. } = node {
                mapping.insert(*op_id, inner.0.borrow().op_metadata().id.unwrap());
            }
        },
    );

    mapping
}

/// Check if the type is serializable. Currently a janky implementation that just looks for common unserializable types.
/// Add to the list as new errors emerge.
fn type_is_serializable(t: &DebugType) -> bool {
    let type_name = t.to_token_stream().to_string();
    let unserializable_types = [
        "Rc",
        "RefCell",
        "Instant",
        "Duration",
        "SystemTime",
        "HashMap",
    ];
    !unserializable_types
        .iter()
        .any(|unser| type_name.contains(unser))
}

/// Returns whether a node can be decoupled.
///
/// False if:
/// 1. The node relies on knowing the initial value (Singleton, KeyedSingleton without BoundedValue)
/// 2. The output type is not serializable
pub fn can_decouple(output_type: &CollectionKind) -> bool {
    !output_type.is_bounded()
        && match output_type {
            CollectionKind::Stream { element_type, .. }
            | CollectionKind::Optional { element_type, .. } => type_is_serializable(element_type),
            CollectionKind::KeyedSingleton {
                bound: KeyedSingletonBoundKind::BoundedValue,
                key_type,
                value_type,
            }
            | CollectionKind::KeyedStream {
                key_type,
                value_type,
                ..
            } => type_is_serializable(key_type) && type_is_serializable(value_type),
            CollectionKind::Singleton { .. } | CollectionKind::KeyedSingleton { .. } => false,
        }
}

#[derive(Clone, PartialEq, Eq)]
pub enum NetworkType {
    Recv,
    Send,
    SendRecv,
}

/// Returns true if the node is at the given location, treating Networks as present
/// at both their send and receive locations.
pub fn node_at_location(node: &HydroNode, location: &LocationId) -> bool {
    if let HydroNode::Network { .. } = node {
        get_network_type(node, location).is_some()
    } else {
        node.metadata().location_id.root() == location.root()
    }
}

pub fn get_network_type(node: &HydroNode, location: &LocationId) -> Option<NetworkType> {
    let mut is_to_us = false;
    let mut is_from_us = false;

    if let HydroNode::Network { input, .. } = node {
        if input.metadata().location_id.root() == location.root() {
            is_from_us = true;
        }
        if node.metadata().location_id.root() == location.root() {
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
            #root::runtime_support::stageleft::runtime_support::fn1_type_hint::<(#root::location::MemberId<_>, #t_type), _>(
                |(id, data)| {
                    (id.into_tagless(), #root::runtime_support::bincode::serialize(&data).unwrap().into())
                }
            )
        }
    } else {
        parse_quote! {
            #root::runtime_support::stageleft::runtime_support::fn1_type_hint::<#t_type, _>(
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

pub fn unbounded_stream(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Stream {
        bound: BoundKind::Unbounded,
        order: StreamOrder::NoOrder,
        retry: StreamRetry::ExactlyOnce,
        element_type: element_type.into(),
    }
}

pub fn bounded_stream(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Stream {
        bound: BoundKind::Bounded,
        order: StreamOrder::NoOrder,
        retry: StreamRetry::ExactlyOnce,
        element_type: element_type.into(),
    }
}

pub fn bounded_optional(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Optional {
        bound: BoundKind::Bounded,
        element_type: element_type.into(),
    }
}

pub fn unbounded_optional(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Optional {
        bound: BoundKind::Unbounded,
        element_type: element_type.into(),
    }
}

pub fn bounded_singleton(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Singleton {
        bound: SingletonBoundKind::Bounded,
        element_type: element_type.into(),
    }
}

pub fn unbounded_singleton(element_type: syn::Type) -> CollectionKind {
    CollectionKind::Singleton {
        bound: SingletonBoundKind::Unbounded,
        element_type: element_type.into(),
    }
}
