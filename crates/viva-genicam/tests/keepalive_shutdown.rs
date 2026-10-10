//! Regression coverage for cancelling keepalive I/O before runtime shutdown.
//! All peers use ephemeral loopback ports and deliberately withhold an ACK.

use std::net::UdpSocket;
use std::time::{Duration, Instant};

use tokio::runtime::{Builder, Runtime};
use viva_genicam::{GigeRegisterIo, gige::GigeDevice};

fn peer() -> UdpSocket {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout");
    socket
}

fn transport(runtime: &Runtime, peer: &UdpSocket) -> GigeRegisterIo {
    let mut device = runtime
        .block_on(GigeDevice::open(peer.local_addr().expect("address")))
        .expect("open");
    device.set_control_timeout(Duration::from_secs(30));
    GigeRegisterIo::new(runtime.handle().clone(), device)
}

/// A worker releases a finished task only after waking its `JoinHandle`, so
/// the alive-task count can trail a completed join by a moment.
fn assert_tasks_released(runtime: &Runtime) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while runtime.metrics().num_alive_tasks() != 0 {
        assert!(Instant::now() < deadline, "keepalive task still alive");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn synchronous_drop_cancels_the_initial_heartbeat_read_and_joins_the_task() {
    let peer = peer();
    let runtime = Runtime::new().expect("runtime");
    let transport = transport(&runtime, &peer);
    peer.recv_from(&mut [0u8; 2048]).expect("heartbeat read");
    let started = Instant::now();
    drop(transport);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_tasks_released(&runtime);
    drop(runtime);
}

#[test]
fn dropping_from_a_multithread_runtime_worker_joins_the_keepalive() {
    let peer = peer();
    let runtime = Runtime::new().expect("runtime");
    let transport = transport(&runtime, &peer);
    peer.recv_from(&mut [0u8; 2048]).expect("heartbeat read");
    runtime
        .block_on(runtime.spawn(async move { drop(transport) }))
        .expect("drop task");
    assert_tasks_released(&runtime);
}

#[test]
fn shutdown_also_cancels_a_periodic_control_ping() {
    let peer = peer();
    let runtime = Runtime::new().expect("runtime");
    let transport = transport(&runtime, &peer);
    let mut request = [0u8; 2048];
    let (_, address) = peer.recv_from(&mut request).expect("heartbeat read");
    // READREG_ACK: a 400 ms heartbeat window selects a 100 ms ping period.
    let mut ack = vec![0, 0, 0, 0x81, 0, 4, request[6], request[7]];
    ack.extend_from_slice(&400u32.to_be_bytes());
    peer.send_to(&ack, address).expect("heartbeat ack");
    peer.recv_from(&mut request).expect("control ping");
    let started = Instant::now();
    drop(transport);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_tasks_released(&runtime);
}

#[tokio::test]
async fn drop_on_a_current_thread_runtime_cancels_io_without_block_in_place() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let mut device = GigeDevice::open(peer.local_addr().expect("address"))
        .await
        .expect("open");
    device.set_control_timeout(Duration::from_secs(30));
    let transport = GigeRegisterIo::new(tokio::runtime::Handle::current(), device);
    tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut [0u8; 2048]))
        .await
        .expect("read deadline")
        .expect("heartbeat read");
    let started = Instant::now();
    drop(transport);
    assert!(started.elapsed() < Duration::from_secs(1));
    tokio::task::yield_now().await;
    assert_eq!(
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks(),
        0
    );
}

#[test]
fn a_current_thread_transport_can_be_dropped_after_block_on_returns() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let transport = runtime.block_on(async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let device = GigeDevice::open(peer.local_addr().expect("address"))
            .await
            .expect("open");
        let transport = GigeRegisterIo::new(runtime.handle().clone(), device);
        tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut [0u8; 2048]))
            .await
            .expect("read deadline")
            .expect("heartbeat read");
        transport
    });
    drop(transport);
    drop(runtime);
}

#[test]
fn a_multithread_transport_can_be_dropped_from_another_current_thread_runtime() {
    let peer = peer();
    let owner = Runtime::new().expect("owning runtime");
    let caller = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("calling runtime");
    let transport = transport(&owner, &peer);
    peer.recv_from(&mut [0u8; 2048]).expect("heartbeat read");
    caller.block_on(async move { drop(transport) });
    assert_tasks_released(&owner);
}
