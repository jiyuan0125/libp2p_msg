use anyhow::anyhow;
use async_std::fs::{OpenOptions};
use async_std::io;
use async_std::io::prelude::BufReadExt;
use clap::Parser;
use futures::executor::block_on;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::AsyncReadExt;
use libp2p::core::multiaddr::{Multiaddr, Protocol};
use libp2p::core::transport::OrTransport;
use libp2p::core::{upgrade, ConnectedPoint};
use libp2p::dns::DnsConfig;
use libp2p::identify::{Identify, IdentifyConfig, IdentifyEvent, IdentifyInfo};
use libp2p::noise;
use libp2p::relay::v2::client::{self, Client};
use libp2p::rendezvous;
use libp2p::swarm::{SwarmBuilder, SwarmEvent};
use libp2p::tcp::{GenTcpConfig, TcpTransport};
use libp2p::Transport;
use libp2p::{dcutr, Swarm};
use libp2p::{identity, NetworkBehaviour, PeerId};
use log::info;
use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::error::Error;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Parser)]
#[clap(name = "libp2p DCUtR client")]
struct Opts {
    /// The listening address
    #[clap(long)]
    relay_address: Multiaddr,
}

#[derive(Debug, Parser, PartialEq)]
enum Mode {
    Dial,
    Listen,
}

impl FromStr for Mode {
    type Err = String;
    fn from_str(mode: &str) -> Result<Self, Self::Err> {
        match mode {
            "dial" => Ok(Mode::Dial),
            "listen" => Ok(Mode::Listen),
            _ => Err("Expected either 'dial' or 'listen'".to_string()),
        }
    }
}

const NAMESPACE: &str = "rendezvous";

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event", event_process = false)]
struct Behaviour {
    relay_client: Client,
    identify: Identify,
    dcutr: dcutr::behaviour::Behaviour,
    sendmsg: libp2p_msg::Behaviour,
    rendezvous: rendezvous::client::Behaviour,

    #[behaviour(ignore)]
    #[allow(dead_code)]
    has_registered: bool,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Event {
    Identify(IdentifyEvent),
    Relay(client::Event),
    Dcutr(dcutr::behaviour::Event),
    Send(libp2p_msg::Event),
    Rendezvous(rendezvous::client::Event),
}

impl From<IdentifyEvent> for Event {
    fn from(e: IdentifyEvent) -> Self {
        Event::Identify(e)
    }
}
impl From<rendezvous::client::Event> for Event {
    fn from(e: rendezvous::client::Event) -> Self {
        Event::Rendezvous(e)
    }
}

impl From<client::Event> for Event {
    fn from(e: client::Event) -> Self {
        Event::Relay(e)
    }
}

impl From<dcutr::behaviour::Event> for Event {
    fn from(e: dcutr::behaviour::Event) -> Self {
        Event::Dcutr(e)
    }
}

impl From<libp2p_msg::Event> for Event {
    fn from(e: libp2p_msg::Event) -> Self {
        Event::Send(e)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut peers: BTreeMap<PeerId, HashSet<ConnectedPoint>> = BTreeMap::new();

    env_logger::init();

    let opts = Opts::parse();

    // relay server  使用固定的peer id, 见server服务器的peerid生成代码 参数 secret_key_seed = 0
    let rendezvous_point = "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN"
        .parse()
        .unwrap();

    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {:?}", local_peer_id);

    let (relay_transport, client) = Client::new_transport_and_behaviour(local_peer_id);

    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&local_key)
        .expect("Signing libp2p-noise static DH keypair failed.");

    let transport = OrTransport::new(
        relay_transport,
        block_on(DnsConfig::system(TcpTransport::new(
            GenTcpConfig::default().port_reuse(true),
        )))
        .unwrap(),
    )
    .upgrade(upgrade::Version::V1)
    .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
    .multiplex(libp2p::yamux::YamuxConfig::default())
    .boxed();

    let behaviour = Behaviour {
        relay_client: client,
        identify: Identify::new(IdentifyConfig::new(
            "/TODO/0.0.1".to_string(),
            local_key.public(),
        )),
        dcutr: dcutr::behaviour::Behaviour::new(),
        sendmsg: libp2p_msg::Behaviour::new(),
        rendezvous: rendezvous::client::Behaviour::new(local_key),

        has_registered: false,
    };

    let mut cookie = None;

    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();

    let mut swarm = SwarmBuilder::new(transport, behaviour, local_peer_id)
        .dial_concurrency_factor(10_u8.try_into().unwrap())
        .build();

    swarm
        .listen_on(
            Multiaddr::empty()
                .with("0.0.0.0".parse::<Ipv4Addr>().unwrap().into())
                .with(Protocol::Tcp(0)),
        )
        .unwrap();

    // Wait to listen on all interfaces.
    block_on(async {
        let mut delay = futures_timer::Delay::new(std::time::Duration::from_millis(100)).fuse();
        loop {
            futures::select! {
                event = swarm.next() => {
                    match event.unwrap() {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            println!("Listening on {:?}", address);
                        }
                        event => panic!("{:?}", event),
                    }
                }

                _ = delay => {
                    // Likely listening on all interfaces now, thus continuing by breaking the loop.
                    break;
                }
            }
        }
    });

    // Connect to the relay server. Not for the reservation or relayed connection, but to (a) learn
    // our local public address and (b) enable a freshly started relay to learn its public address.
    swarm.dial(opts.relay_address.clone()).unwrap();

    block_on(async {
        let mut learned_observed_addr = false;
        let mut told_relay_observed_addr = false;

        loop {
            match swarm.next().await.unwrap() {
                SwarmEvent::NewListenAddr { .. } => {}
                SwarmEvent::Dialing { .. } => {}

                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    info!("{}", peer_id);
                }

                SwarmEvent::Behaviour(Event::Identify(IdentifyEvent::Sent { .. })) => {
                    info!("Told relay its public address.");
                    told_relay_observed_addr = true;
                }
                SwarmEvent::Behaviour(Event::Identify(IdentifyEvent::Received {
                    info: IdentifyInfo { observed_addr, .. },
                    ..
                })) => {
                    println!("Relay told us our public address: {:?}", observed_addr);
                    learned_observed_addr = true;

                    swarm.behaviour_mut().rendezvous.register(
                        rendezvous::Namespace::from_static("rendezvous"),
                        rendezvous_point,
                        None,
                    );
                }
                event => info!("{:?}", event),
            }

            if learned_observed_addr && told_relay_observed_addr {
                break;
            }
        }
    });

    swarm
        .listen_on(opts.relay_address.clone().with(Protocol::P2pCircuit))
        .unwrap();

    block_on(async {
        loop {
            futures::select! {
                line = stdin.select_next_some() => {
                    let line = line.expect("Stdin ont to close");
                    match Command::try_from(line.as_str()) {
                        Ok(Command::ListPeers) => handle_list_peers(&peers).await,
                        Ok(Command::SendFile { peer_id, file_path }) => { 
                            if let Err(e) = handle_send_file(&mut swarm, peer_id, file_path).await {
                                eprintln!("Error: {:?}", e);
                            }
                        }
                        Err(_) => eprintln!("Wrong command, available commans are: ls, file <PeerId> <File Path>"),
                        _ => {}
                    }
                }

                event = swarm.select_next_some() => match event{
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Listening on {:?}", address);
                    }
                    SwarmEvent::Behaviour(Event::Relay(client::Event::ReservationReqAccepted {
                        ..
                    })) => {
                        info!("Relay accepted our reservation request.");
                    }
                    SwarmEvent::Behaviour(Event::Relay(event)) => {
                        info!("{:?}", event)
                    }
                    SwarmEvent::Behaviour(Event::Dcutr(event)) => {
                        info!("{:?}", event)
                    }
                    SwarmEvent::Behaviour(Event::Send(event)) => {
                        println!("{:?}", event)
                    }
                    SwarmEvent::Behaviour(Event::Identify(event)) => {
                        info!("{:?}", event)
                    }
                    SwarmEvent::Behaviour(Event::Rendezvous(rendezvous::client::Event::Registered {
                        namespace,
                        ttl,
                        rendezvous_node,
                    })) => {
                        println!(
                            "Registered for namespace '{}' at rendezvous point {} for the next {} seconds",
                            namespace,
                            rendezvous_node,
                            ttl
                        );
                        swarm.behaviour_mut().has_registered = true;

                        let behaviour = swarm.behaviour_mut();

                        behaviour.rendezvous.discover(
                            Some(rendezvous::Namespace::new(NAMESPACE.to_string()).unwrap()),
                            None,
                            None,
                            rendezvous_point
                        );

                    }
                    SwarmEvent::Behaviour(Event::Rendezvous(rendezvous::client::Event::Discovered {
                            registrations,
                            cookie: new_cookie,
                            ..
                    })) => {
                        cookie.replace(new_cookie);

                        for registration in registrations {
                            for address in registration.record.addresses() {
                                let peer = registration.record.peer_id();
                                println!("Discovered peer {} at {}", peer, address);

                                let p2p_suffix = Protocol::P2p(*peer.as_ref());
                                let _address_with_p2p =
                                    if !address.ends_with(&Multiaddr::empty().with(p2p_suffix.clone())) {
                                        address.clone().with(p2p_suffix)
                                    } else {
                                        address.clone()
                                    };

                                //swarm.dial(address_with_p2p).unwrap()
                                swarm
                                .dial(
                                    opts.relay_address.clone()
                                    .with(Protocol::P2pCircuit)
                                    .with(Protocol::P2p(peer.into())),
                                    )
                                    .unwrap();
                                println!("Dial {}",opts.relay_address.clone()
                                    .with(Protocol::P2pCircuit)
                                    .with(Protocol::P2p(peer.into())) );
                            }
                        }
                    }

                    SwarmEvent::ConnectionEstablished {
                        peer_id, endpoint, ..
                    } => {
                        println!("Established connection to {:?} via {:?}", peer_id, endpoint);
                        // swarm.behaviour_mut()
                        //     .sendmsg
                        //     .insert(&peer_id);

                        peers.entry(peer_id).or_default().insert(endpoint);

                        let peers = swarm.connected_peers();
                        for p in peers {
                            println!("peer {}",p);
                        }
                    }

                    SwarmEvent::ConnectionClosed { peer_id, endpoint ,.. } => {
                        println!("disconnect {:?} by {:?}", peer_id, endpoint);
                        /*swarm.behaviour_mut()
                        .sendmsg
                        .remove(&peer_id);*/
                        if let Some(eps) = peers.get_mut(&peer_id) {
                            eps.remove(&endpoint);
                            if eps.is_empty() {
                                peers.remove(&peer_id);
                            }
                        }
                    },


                    SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                        info!("Outgoing connection error to {:?}: {:?}", peer_id, error);
                    }
                    _ => {}
                }
            } //select
        } //loop
    })
}

#[derive(Debug)]
enum Command {
    ListPeers,
    SendFile { peer_id: PeerId, file_path: PathBuf },
    Unknown,
}

impl<'a> TryFrom<&'a str> for Command {
    type Error = anyhow::Error;

    fn try_from(line: &'a str) -> Result<Self, Self::Error> {
        let mut tokens = line.splitn(3, ' ');
        match tokens.next() {
            // 解析 ls 命令
            Some(token) if token == "ls" => Ok(Command::ListPeers),
            // 解析发送文件命令
            Some(token) if token == "file" => {
                let (peer_id, file_path) = {
                    match (tokens.next(), tokens.next()) {
                        (Some(peer_id), Some(file_path)) => (peer_id, file_path),
                        _ => return Err(anyhow!("Failed to parse peer_id or file_path")),
                    }
                };
                let peer_id = peer_id
                    .parse()
                    .map_err(|_| anyhow!("Failed to parse peer_id from &str"))?;
                let file_path = file_path
                    .parse()
                    .map_err(|_| anyhow!("Failed to parse file_path from &str"))?;

                Ok(Command::SendFile { peer_id, file_path })
            }
            _ => Ok(Command::Unknown),
        }
    }
}

async fn handle_list_peers(peers: &BTreeMap<PeerId, HashSet<ConnectedPoint>>) {
    peers.keys().for_each(|peer| {
        println!("peer: {}", peer);
    });
}

async fn handle_send_file(
    swarm: &mut Swarm<Behaviour>,
    peer_id: PeerId,
    file_path: PathBuf,
) -> anyhow::Result<()> {
    let mut file = OpenOptions::new().read(true).open(file_path).await?;
    let mut buf = [0; 1024];
    loop {
        let n = file.read(&mut buf).await?;
        swarm.behaviour_mut().sendmsg.send(buf, peer_id);
        if n == 0 {
            break;
        }
    }
    Ok(())
}
