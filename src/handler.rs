use crate::protocol;
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, ConnectionHandlerUpgrErr, KeepAlive,
    SubstreamProtocol,
};
use std::collections::VecDeque;
use std::task::{Context, Poll};

#[derive(Debug)]
pub enum Success {
    OK,
}

pub struct Handler {
    /// Outbound Inbound events
    #[allow(clippy::type_complexity)]
    queued_events: VecDeque<
        ConnectionHandlerEvent<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::OutEvent,
            <Self as ConnectionHandler>::Error,
        >,
    >,
}

impl Handler {
    pub fn new() -> Self {
        Handler {
            queued_events: Default::default(),
        }
    }
}

impl Default for Handler {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionHandler for Handler {
    type InEvent = protocol::MsgContent;
    type OutEvent = protocol::MsgContent;
    type Error = std::io::Error;
    type InboundProtocol = protocol::MsgContent;
    type OutboundProtocol = protocol::MsgContent;
    type OutboundOpenInfo = ();
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<protocol::MsgContent, ()> {
        SubstreamProtocol::new(
            protocol::MsgContent {
                data: Default::default(),
            },
            (),
        )
    }

    //protocol::InboundUpgrade::Output
    fn inject_fully_negotiated_inbound(&mut self, output: Vec<u8>, (): ()) {
        self.queued_events
            .push_back(ConnectionHandlerEvent::Custom(protocol::MsgContent {
                data: output,
            }));
    }

    fn inject_fully_negotiated_outbound(&mut self, _output: protocol::Success, (): ()) {
    }

    fn inject_event(&mut self, msg: protocol::MsgContent) {
        //println!("handler inject event ");
        self.queued_events
            .push_back(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(msg, ()),
            });
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: (),
        _error: ConnectionHandlerUpgrErr<std::io::Error>,
    ) {
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        KeepAlive::Yes
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<ConnectionHandlerEvent<protocol::MsgContent, (), protocol::MsgContent, Self::Error>>
    {
        if let Some(msg) = self.queued_events.pop_back() {
            return Poll::Ready(msg);
        }

        Poll::Pending
    }
}
