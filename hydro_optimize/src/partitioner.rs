use core::panic;
use std::collections::HashMap;

use hydro_lang::compile::ir::{HydroNode, HydroRoot, HydroSource, traverse_dfir};
use hydro_lang::location::dynamic::LocationId;
use serde::{Deserialize, Serialize};
use syn::visit_mut::VisitMut;

use crate::partition_syn_analysis::StructOrTupleIndex;
use crate::rewrites::{
    ClusterSelfIdReplace, NetworkType, collection_kind_to_debug_type,
    deserialize_bincode_with_type, get_network_type, prepend_member_id_to_collection_kind,
    serialize_bincode_with_type,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Partitioner {
    pub nodes_before_partitioned_input: HashMap<usize, StructOrTupleIndex>, /* ID of node right before a Network -> what to partition on */
    pub num_partitions: usize,
    pub location_id: LocationId,
    pub new_cluster_id: Option<LocationId>, /* If we're partitioning a process, then a new cluster will be created and we'll need to substitute the old ID with this one */
}

pub fn quoted_struct_or_tuple_index(
    mut tuple: syn::Expr,
    indices: &StructOrTupleIndex,
) -> syn::Expr {
    for index in indices {
        let member = if let Ok(num_index) = index.parse::<usize>() {
            syn::Member::Unnamed(syn::Index::from(num_index))
        } else {
            syn::Member::Named(syn::Ident::new(index, proc_macro2::Span::call_site()))
        };

        let dot_token = <syn::Token![.]>::default();

        tuple = syn::Expr::Field(syn::ExprField {
            attrs: vec![],
            base: Box::new(tuple),
            dot_token,
            member,
        });
    }
    tuple
}

fn replace_sender_dest(node: &mut HydroNode, partitioner: &Partitioner, op_id: usize) {
    let Partitioner {
        nodes_before_partitioned_input: nodes_to_partition,
        num_partitions,
        new_cluster_id,
        ..
    } = partitioner;

    if let Some(indices) = nodes_to_partition.get(&op_id) {
        println!("Replacing sender destination at {}", op_id);

        let node_content = std::mem::replace(node, HydroNode::Placeholder);
        let mut metadata = node_content.metadata().clone();
        let element_type: syn::Type =
            (*collection_kind_to_debug_type(&metadata.collection_kind).0).clone();

        let struct_or_tuple = syn::parse_quote! { struct_or_tuple };
        let struct_or_tuple_with_fields = quoted_struct_or_tuple_index(struct_or_tuple, indices);

        let f: syn::Expr = if new_cluster_id.is_some() {
            // Output type of Map now includes dest ID
            metadata.collection_kind =
                prepend_member_id_to_collection_kind(&metadata.collection_kind);

            // Partitioning a process into a cluster
            syn::parse_quote!(
                |struct_or_tuple: #element_type| {
                    let mut s = ::std::hash::DefaultHasher::new();
                    ::std::hash::Hash::hash(&#struct_or_tuple_with_fields, &mut s);
                    let partition_val = ::std::hash::Hasher::finish(&s) as u32;
                    let dest = (partition_val % #num_partitions as u32) as u32;
                    (
                        hydro_lang::location::MemberId::<()>::from_raw_id(dest),
                        struct_or_tuple
                    )
                }
            )
        } else {
            // Already a cluster — element_type is (MemberId, T) for KeyedStream
            syn::parse_quote!(
                |(orig_dest, struct_or_tuple): #element_type| {
                    let mut s = ::std::hash::DefaultHasher::new();
                    ::std::hash::Hash::hash(&#struct_or_tuple_with_fields, &mut s);
                    let partition_val = ::std::hash::Hasher::finish(&s) as u32;
                    let orig_raw = orig_dest.into_tagless().get_raw_id();
                    let dest = (orig_raw * #num_partitions as u32) + (partition_val % #num_partitions as u32) as u32;
                    (
                        hydro_lang::location::MemberId::<()>::from_raw_id(dest),
                        struct_or_tuple
                    )
                }
            )
        };

        let mapped_node = HydroNode::Map {
            f: f.into(),
            input: Box::new(node_content),
            metadata,
        };

        *node = mapped_node;
    }
}

fn replace_receiver_src_id(node: &mut HydroNode, partitioner: &Partitioner, op_id: usize) {
    let Partitioner {
        num_partitions,
        location_id,
        ..
    } = partitioner;

    if let HydroNode::Network {
        input, metadata, ..
    } = node
        && input.metadata().location_id.root() == location_id.root()
    {
        println!(
            "Rewriting network on op {} so the sender's ID is mapped from the partition to the original sender",
            op_id
        );

        let metadata = metadata.clone();
        let node_content = std::mem::replace(node, HydroNode::Placeholder);
        let f: syn::Expr = syn::parse_quote!(|(sender_id, b)| (
            hydro_lang::location::MemberId::<_>::from_raw_id(sender_id.into_tagless().get_raw_id() / #num_partitions as u32),
            b
        ));

        let mapped_node = HydroNode::Map {
            f: f.into(),
            input: Box::new(node_content),
            metadata: metadata.clone(),
        };

        *node = mapped_node;
    }
}

fn replace_network_serialization(node: &mut HydroNode, partitioner: &Partitioner, op_id: usize) {
    let Partitioner { new_cluster_id, .. } = partitioner;

    if let Some(network_type) = get_network_type(node, new_cluster_id.as_ref().unwrap()) {
        let HydroNode::Network {
            serialize_fn,
            deserialize_fn,
            metadata,
            ..
        } = node
        else {
            panic!("Expected a HydroNode::Network, but found {:?}", node);
        };

        let output_type = collection_kind_to_debug_type(&metadata.collection_kind);

        // The partitioned process (now cluster) is the sender
        // Its ID will now be in the recipient's output, so change the deserialize fn
        match network_type {
            NetworkType::Send | NetworkType::SendRecv => {
                println!(
                    "Replacing deserialize function for op {} to include partitioned cluster ID",
                    op_id
                );
                let unit_type: syn::Type = syn::parse_quote! { () }; // Assume we are a Cluster<()>
                let new_deserialize_fn =
                    deserialize_bincode_with_type(Some(&unit_type), &output_type);
                *deserialize_fn = Some(new_deserialize_fn.into());
            }
            _ => {}
        }

        // The partitioned process (now cluster) is the receiver
        // The sender will have to specify its ID, so change the serialize fn
        match network_type {
            NetworkType::Recv | NetworkType::SendRecv => {
                println!(
                    "Replacing serialize function for op {} to include partitioned cluster ID",
                    op_id
                );
                let new_serialize_fn = serialize_bincode_with_type(true, &output_type);
                *serialize_fn = Some(new_serialize_fn.into());
            }
            _ => {}
        }
    }
}

/// If we're partitioning a process into a cluster, we need to replace references to its location
fn replace_process_node_location(node: &mut HydroNode, partitioner: &Partitioner) {
    let Partitioner {
        location_id,
        new_cluster_id,
        ..
    } = partitioner;

    if let Some(new_id) = new_cluster_id {
        // Change any HydroNodes with a location field
        let location = &mut node.metadata_mut().location_id;
        if location.root() == location_id.root() {
            location.swap_root(new_id.clone());
        }
    }
}

/// If we're partitioning a process into a cluster, we need to remove the default sender ID on outgoing networks
fn remove_sender_id_from_receiver(node: &mut HydroNode, partitioner: &Partitioner, op_id: usize) {
    let Partitioner { new_cluster_id, .. } = partitioner;

    let network_type = get_network_type(node, new_cluster_id.as_ref().unwrap());
    match network_type {
        Some(NetworkType::Send) | Some(NetworkType::SendRecv) => {
            println!("Removing sender ID from receiver for op {}", op_id);
            let metadata = node.metadata().clone();
            let node_content = std::mem::replace(node, HydroNode::Placeholder);
            let f: syn::Expr = syn::parse_quote!(|(_sender_id, b)| b);

            let mapped_node = HydroNode::Map {
                f: f.into(),
                input: Box::new(node_content),
                metadata,
            };
            *node = mapped_node;
        }
        _ => {}
    }
}

/// Broadcasting to the partitioned cluster should not actually send a message to every partition.
fn replace_cluster_members_source(node: &mut HydroNode, partitioner: &Partitioner) {
    let num_partitions = partitioner.num_partitions;
    let location_key = partitioner.location_id.key();

    if let HydroNode::Source {
        source: HydroSource::ClusterMembers(target_loc, _),
        ..
    } = node
        && target_loc.root().key() == location_key
    {
        let source_node = std::mem::replace(node, HydroNode::Placeholder);
        let source_metadata = source_node.metadata().clone();
        *node = HydroNode::FilterMap {
            f: {
                let e: syn::Expr = syn::parse_quote!(
                    |(id, event): (hydro_lang::location::TaglessMemberId, _)| {
                        let raw = id.get_raw_id();
                        if raw % #num_partitions as u32 == 0 {
                            Some((hydro_lang::location::TaglessMemberId::from_raw_id(raw / #num_partitions as u32), event))
                        } else {
                            None
                        }
                    }
                );
                e.into()
            },
            input: Box::new(source_node),
            metadata: source_metadata,
        };
    }
}

fn partition_node(node: &mut HydroNode, partitioner: &Partitioner) {
    let Some(op_id) = node.op_metadata().id else {
        return;
    };

    // Rewrite self-ID references for the partitioned cluster
    node.visit_debug_expr(|expr| {
        let mut visitor = ClusterSelfIdReplace::Partition {
            num_partitions: partitioner.num_partitions,
            partitioned_cluster_id: partitioner.location_id.key(),
            op_id,
        };
        visitor.visit_expr_mut(&mut expr.0);
    });

    replace_cluster_members_source(node, partitioner);
    replace_sender_dest(node, partitioner, op_id);

    // Change process into cluster
    if partitioner.new_cluster_id.is_some() {
        replace_process_node_location(node, partitioner);
    } else {
        replace_receiver_src_id(node, partitioner, op_id);
    }
}

/// Limitations: Can only partition 1 cluster at a time. Assumes that the partitioned attribute can be casted to usize.
pub fn partition(ir: &mut [HydroRoot], partitioner: &Partitioner) {
    traverse_dfir(
        ir,
        |_, _| {},
        |node, _| {
            partition_node(node, partitioner);
        },
    );

    if partitioner.new_cluster_id.is_some() {
        // DANGER: Do not depend on the ID here, since nodes would've been injected
        // Fix network only after all IDs have been replaced, since get_network_type relies on it
        traverse_dfir(
            ir,
            |_, _| {},
            |node, _| {
                let Some(op_id) = node.op_metadata().id else {
                    return;
                };
                replace_network_serialization(node, partitioner, op_id);
                remove_sender_id_from_receiver(node, partitioner, op_id);
            },
        );
    }
}
