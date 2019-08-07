use super::{Blockchain, Error, ErrorKind, PreCheckedHeader, Ref};
use crate::blockcfg::{Block, Header, HeaderHash};
use crate::intercom::{self, NetworkMsg};
use crate::network::p2p::topology::NodeId;
use crate::utils::async_msg::MessageBox;
use chain_core::property::HasHeader;

use futures::future::{self, Either};
use futures::prelude::*;
use slog::Logger;

pub fn process_leadership_block(
    mut blockchain: Blockchain,
    block: Block,
    parent: Ref,
    logger: Logger,
) -> impl Future<Item = Ref, Error = Error> {
    let header = block.header();
    // This is a trusted block from the leadership task,
    // so we can skip pre-validation.
    blockchain
        .post_check_header(header, parent)
        .and_then(move |post_checked| blockchain.apply_block(post_checked, block))
}

pub fn process_block_announcement(
    mut blockchain: Blockchain,
    header: Header,
    node_id: NodeId,
    mut network_msg_box: MessageBox<NetworkMsg>,
    logger: Logger,
) -> impl Future<Item = (), Error = Error> {
    blockchain
        .pre_check_header(header)
        .and_then(move |pre_checked| match pre_checked {
            PreCheckedHeader::AlreadyPresent { .. } => {
                debug!(logger, "block is already present");
                Either::A(future::ok(()))
            }
            PreCheckedHeader::MissingParent { header, .. } => {
                debug!(logger, "block is missing a locally stored parent");
                let to = header.hash();
                Either::B(blockchain.get_checkpoints(to).map(move |from| {
                    network_msg_box
                        .try_send(NetworkMsg::PullHeaders { node_id, from, to })
                        .unwrap_or_else(move |err| {
                            error!(
                                logger,
                                "cannot send PullHeaders request to network: {}", err
                            )
                        });
                }))
            }
            PreCheckedHeader::HeaderWithCache { header, parent_ref } => {
                debug!(logger, "Block announcement is interesting, fetch block");
                network_msg_box
                    .try_send(NetworkMsg::GetNextBlock(node_id, header.hash()))
                    .unwrap_or_else(move |err| {
                        error!(
                            logger,
                            "cannot send GetNextBlock request to network: {}", err
                        )
                    });
                Either::A(future::ok(()))
            }
        })
}

pub fn process_network_block(
    mut blockchain: Blockchain,
    block: Block,
    mut network_msg_box: MessageBox<NetworkMsg>,
    logger: Logger,
) -> impl Future<Item = (), Error = Error> {
    let mut end_blockchain = blockchain.clone();
    let header = block.header();
    blockchain
        .pre_check_header(header)
        .and_then(move |pre_checked| match pre_checked {
            PreCheckedHeader::AlreadyPresent { .. } => {
                debug!(logger, "block is already present");
                Either::A(future::ok(()))
            }
            PreCheckedHeader::MissingParent { header, .. } => {
                debug!(logger, "block is missing a locally stored parent");
                Either::A(future::err(
                    ErrorKind::MissingParentBlockFromStorage(header).into(),
                ))
            }
            PreCheckedHeader::HeaderWithCache { header, parent_ref } => {
                let post_check_and_apply = blockchain
                    .post_check_header(header, parent_ref)
                    .and_then(move |post_checked| end_blockchain.apply_block(post_checked, block))
                    .map(move |_| {
                        // TODO: advance branch?
                        debug!(logger, "block successfully applied");
                    });
                Either::B(post_check_and_apply)
            }
        })
}
