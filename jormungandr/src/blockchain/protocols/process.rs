use super::{Blockchain, Branch, Error, ErrorKind, PreCheckedHeader, Ref};
use crate::blockcfg::{Block, Header, HeaderHash};
use crate::intercom::{self, NetworkMsg};
use crate::network::p2p::topology::NodeId;
use crate::utils::async_msg::MessageBox;
use chain_core::property::HasHeader;

use futures::future::{self, Either};
use futures::prelude::*;
use futures::stream;
use slog::Logger;

use std::convert::identity;

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
    branch: Branch,
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
                Either::B(blockchain.get_checkpoints(branch).map(move |from| {
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
                debug!(
                    logger,
                    "Announced block has a locally stored parent, fetch it"
                );
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

pub fn process_chain_headers_into_block_request<S>(
    mut blockchain: Blockchain,
    headers: S,
    logger: Logger,
) -> impl Future<Item = Vec<HeaderHash>, Error = Error>
where
    S: Stream<Item = Header>,
{
    headers
        .map_err(|e| {
            // TODO: map the incoming stream error to the result error
            unimplemented!()
        })
        .and_then(move |header| {
            blockchain
                .pre_check_header(header)
                .and_then(move |pre_checked| match pre_checked {
                    PreCheckedHeader::AlreadyPresent { .. } => {
                        // The block is already present. This may happen
                        // if the peer has started from an earlier checkpoint
                        // than our tip, so ignore this and proceed.
                        Ok(None)
                    }
                    PreCheckedHeader::MissingParent { header, .. } => {
                        // TODO: this fails on the first header after the
                        // immediate descendant of the local tip. Need branch storage
                        // that would store the whole header chain without blocks,
                        // so that the chain can be pre-validated first and blocks
                        // fetched afterwards in arbitrary order.
                        Err(ErrorKind::MissingParentBlockFromStorage(header).into())
                    }
                    PreCheckedHeader::HeaderWithCache { header, parent_ref } => {
                        // TODO: limit the headers to the single epoch
                        // before pausing to retrieve blocks.
                        Ok(Some(header.hash()))
                    }
                })
        })
        .filter_map(identity)
        .collect()
}
