use std::collections::HashMap;

use hydro_lang::compile::builder::FlowBuilder;
use hydro_lang::compile::ir::{
    BoundKind, CollectionKind, HydroIrMetadata, HydroIrOpMetadata, HydroNode, HydroRoot,
    SharedNode, SingletonBoundKind, StreamOrder, StreamRetry, traverse_dfir,
};
use serde::{Deserialize, Serialize};

use crate::partial_partitioner::{PartialPartitioner, partial_partition};
use crate::partition_syn_analysis::StructOrTupleIndex;
use crate::partitioner::quoted_struct_or_tuple_index;
use crate::rewrites::collection_kind_to_debug_type;

#[derive(Clone, Serialize, Deserialize)]
pub struct PartialPartitionerSealed {
    pub inner: PartialPartitioner,
    /// (node ID before Network at partition, node ID after Network at recipient)
    /// Identifies outputs from partitioned nodes that contain HashMaps needing merge.
    pub nodes_to_seal: Vec<(usize, usize)>,
}

/// Recursively find the first HashMap field in a syn::Type, returning its StructOrTupleIndex path.
pub fn find_hashmap_index(ty: &syn::Type) -> Option<StructOrTupleIndex> {
    match ty {
        syn::Type::Tuple(tuple) => {
            for (i, elem) in tuple.elems.iter().enumerate() {
                if let Some(mut index) = find_hashmap_index(elem) {
                    index.insert(0, i.to_string());
                    return Some(index);
                }
            }
            None
        }
        syn::Type::Path(path) => {
            let seg = path.path.segments.last()?;
            if seg.ident == "HashMap" {
                Some(vec![])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Insert a Map before the network that tags output with the partition's logical clock,
/// and a Scan after the network that merges HashMaps and emits once all partitions respond.
pub fn partial_partition_sealed(
    builder: &mut FlowBuilder,
    ir: &mut Vec<HydroRoot>,
    partitioner: PartialPartitionerSealed,
) {
    let PartialPartitionerSealed { inner, nodes_to_seal } = partitioner;
    let num_partitions = inner.num_partitions;

    // Run the core partial partitioning (coordinator, partition state, replicated/partitioned handling,
    // inject_id, and regular partitioning). Returns the inner state when replicated nodes exist.
    let state = partial_partition(builder, ir, inner);

    if nodes_to_seal.is_empty() {
        return;
    }

    let state = state.expect("nodes_to_seal requires replicated nodes in the partitioner");
    let p_state_tee = state.p_state_tee.as_ref().unwrap().clone();
    let partition_tick = &state.partition_tick;

    let before_set: HashMap<usize, usize> = nodes_to_seal.iter().copied().collect();
    let mut sealed_types: HashMap<usize, (syn::Type, StructOrTupleIndex)> = HashMap::new();

    // First pass: before each sealed network, CrossSingleton with p_state_tee then Map to (clock, value).
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            let Some(op_id) = node.op_metadata().id else {
                return;
            };
            if let Some(&after_id) = before_set.get(&op_id) {
                let metadata = node.metadata().clone();
                let element_type: syn::Type =
                    (*collection_kind_to_debug_type(&metadata.collection_kind).0).clone();
                let hashmap_index = find_hashmap_index(&element_type)
                    .expect("No HashMap found in sealed node's element type");
                sealed_types.insert(after_id, (element_type.clone(), hashmap_index));

                let bt = &metadata;
                let state_type: syn::Type = syn::parse_quote! { (bool, usize) };
                let with_state_type: syn::Type =
                    syn::parse_quote! { (#element_type, (bool, usize)) };
                let clocked_type: syn::Type = syn::parse_quote! { (usize, #element_type) };

                let node_content = std::mem::replace(node, HydroNode::Placeholder);

                // Tee the partition state to read the logical clock
                let p_state = HydroNode::Tee {
                    inner: SharedNode(p_state_tee.clone()),
                    metadata: HydroIrMetadata {
                        location_id: partition_tick.clone(),
                        collection_kind: CollectionKind::Singleton {
                            bound: SingletonBoundKind::Bounded,
                            element_type: state_type.into(),
                        },
                        cardinality: None,
                        tag: None,
                        op: HydroIrOpMetadata {
                            backtrace: bt.op.backtrace.clone(),
                            cpu_usage: None,
                            network_recv_cpu_usage: None,
                            id: None,
                        },
                    },
                };

                let cross = HydroNode::CrossSingleton {
                    left: Box::new(node_content),
                    right: Box::new(p_state),
                    metadata: HydroIrMetadata {
                        collection_kind: CollectionKind::Stream {
                            bound: BoundKind::Bounded,
                            order: StreamOrder::NoOrder,
                            retry: StreamRetry::ExactlyOnce,
                            element_type: with_state_type.into(),
                        },
                        ..metadata.clone()
                    },
                };

                *node = HydroNode::Map {
                    f: {
                        let e: syn::Expr = syn::parse_quote!(
                            |(value, (_is_idle, clock)): (#element_type, (bool, usize))| (clock, value)
                        );
                        e.into()
                    },
                    input: Box::new(cross),
                    metadata: HydroIrMetadata {
                        collection_kind: CollectionKind::Stream {
                            bound: BoundKind::Bounded,
                            order: StreamOrder::NoOrder,
                            retry: StreamRetry::ExactlyOnce,
                            element_type: clocked_type.into(),
                        },
                        ..metadata
                    },
                };
            }
        },
    );

    // Second pass: after each sealed network, insert Scan that merges HashMaps and emits on quorum.
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            let Some(op_id) = node.op_metadata().id else {
                return;
            };
            if let Some((element_type, hashmap_index)) = sealed_types.get(&op_id) {
                insert_merge_scan(node, element_type, hashmap_index, num_partitions);
            }
        },
    );
}

/// Insert a Scan + FilterMap after the network node that:
/// 1. ReduceKeyed (via Scan state) on logical clock, merging HashMap fields
/// 2. Buffers until all partitions respond
/// 3. Emits the merged result
fn insert_merge_scan(
    node: &mut HydroNode,
    element_type: &syn::Type,
    hashmap_index: &StructOrTupleIndex,
    num_partitions: usize,
) {
    let metadata = node.metadata().clone();

    // Build field access expressions for the HashMap merge
    let existing_access = quoted_struct_or_tuple_index(syn::parse_quote!(existing), hashmap_index);
    let value_access = quoted_struct_or_tuple_index(syn::parse_quote!(value), hashmap_index);

    let scan_output_type: syn::Type = syn::parse_quote! { Option<#element_type> };

    let node_content = std::mem::replace(node, HydroNode::Placeholder);

    let scan_node = HydroNode::Scan {
        init: {
            let e: syn::Expr = syn::parse_quote!(|| std::collections::HashMap::<usize, (Option<#element_type>, usize)>::new());
            e.into()
        },
        acc: {
            let e: syn::Expr = syn::parse_quote!(
                |state: &mut std::collections::HashMap<usize, (Option<#element_type>, usize)>,
                 (clock, value): (usize, #element_type)|
                 -> Option<#element_type> {
                    let (acc, count) = state.entry(clock).or_insert((None, 0));
                    match acc {
                        None => { *acc = Some(value); }
                        Some(existing) => { #existing_access.extend(#value_access); }
                    }
                    *count += 1;
                    if *count == #num_partitions {
                        let (result, _) = state.remove(&clock).unwrap();
                        result
                    } else {
                        None
                    }
                }
            );
            e.into()
        },
        input: Box::new(node_content),
        metadata: HydroIrMetadata {
            collection_kind: CollectionKind::Stream {
                bound: BoundKind::Unbounded,
                order: StreamOrder::NoOrder,
                retry: StreamRetry::ExactlyOnce,
                element_type: scan_output_type.into(),
            },
            ..metadata.clone()
        },
    };

    // FilterMap to extract Some values
    *node = HydroNode::FilterMap {
        f: {
            let e: syn::Expr = syn::parse_quote!(|x| x);
            e.into()
        },
        input: Box::new(scan_node),
        metadata: HydroIrMetadata {
            collection_kind: metadata.collection_kind.clone(),
            ..metadata
        },
    };
}
