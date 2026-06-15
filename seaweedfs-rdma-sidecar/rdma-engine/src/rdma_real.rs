//! Real RDMA datapath over libibverbs + librdmacm (`rdma_cm`).
//!
//! Phase-1 proof-of-plumbing: a genuine `IBV_WR_RDMA_READ` (via the librdmacm
//! convenience verb `rdma_post_read`) that pulls *real* bytes out of a remote
//! peer's registered memory region over SoftRoCE (rxe) — replacing the mock
//! engine that fabricated `byte = i % 256` and never touched a wire.
//!
//! [`RealRdmaContext`] is the client (the engine's real backend: connect /
//! register_memory / post_read / poll_completion / device_info); [`RealResponder`]
//! is a throwaway loopback target that registers a buffer for `REMOTE_READ` and
//! hands the client its `{addr, rkey, len}`. Connection setup and QP state
//! transitions are handled by `rdma_cm` (much simpler than a manual GID/QPN/LID
//! handshake for RoCEv2). Linux + libibverbs/librdmacm only (`real-rdma` feature).

#![allow(clippy::missing_safety_doc)]

use std::ffi::{CStr, CString};
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;

use rdma_sys::*;

use crate::error::{RdmaError, RdmaResult};
use crate::rdma::{CompletionStatus, MemoryRegion, RdmaDeviceInfo, RdmaOp, WorkCompletion};

/// Metadata the responder hands the client so it can RDMA-READ the source MR.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RemoteInfo {
    addr: u64,
    rkey: u32,
    len: u32,
}

const META_LEN: usize = std::mem::size_of::<RemoteInfo>();

fn op_failed(op: &str) -> RdmaError {
    RdmaError::OperationFailed {
        operation: format!("{op}: {}", std::io::Error::last_os_error()),
        status: -1,
    }
}

unsafe fn qp_init_attr() -> ibv_qp_init_attr {
    let mut a: ibv_qp_init_attr = std::mem::zeroed();
    a.cap.max_send_wr = 8;
    a.cap.max_recv_wr = 8;
    a.cap.max_send_sge = 1;
    a.cap.max_recv_sge = 1;
    a.sq_sig_all = 1;
    a
}

// Allow at least one outstanding RDMA READ in each direction.
unsafe fn conn_param() -> rdma_conn_param {
    let mut cp: rdma_conn_param = std::mem::zeroed();
    cp.responder_resources = 1;
    cp.initiator_depth = 1;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;
    cp
}

unsafe fn query_device(id: *mut rdma_cm_id) -> RdmaDeviceInfo {
    let ctx = (*id).verbs;
    let mut attr: ibv_device_attr = std::mem::zeroed();
    let mut name = String::from("unknown");
    if !ctx.is_null() {
        ibv_query_device(ctx, &mut attr);
        let dev = (*ctx).device;
        if !dev.is_null() {
            name = CStr::from_ptr((*dev).name.as_ptr())
                .to_string_lossy()
                .into_owned();
        }
    }
    RdmaDeviceInfo {
        name,
        vendor_id: attr.vendor_id,
        vendor_part_id: attr.vendor_part_id,
        hw_ver: attr.hw_ver,
        max_mr: attr.max_mr as u32,
        max_qp: attr.max_qp as u32,
        max_cq: attr.max_cq as u32,
        max_mr_size: attr.max_mr_size,
        port_gid: String::new(),
        port_lid: 0,
    }
}

/// Busy-poll a librdmacm completion helper (`rdma_get_send_comp` /
/// `rdma_get_recv_comp`) until it returns a completion or an error.
unsafe fn wait_comp(
    f: unsafe fn(*mut rdma_cm_id, *mut ibv_wc) -> c_int,
    id: *mut rdma_cm_id,
    what: &str,
) -> RdmaResult<ibv_wc> {
    let mut wc: ibv_wc = std::mem::zeroed();
    loop {
        let n = f(id, &mut wc);
        if n < 0 {
            return Err(op_failed(what));
        }
        if n > 0 {
            break;
        }
    }
    if wc.status != ibv_wc_status::IBV_WC_SUCCESS {
        return Err(RdmaError::OperationFailed {
            operation: format!("{what}: wc status {}", wc.status),
            status: wc.status as i32,
        });
    }
    Ok(wc)
}

/// The real RDMA client backend.
pub struct RealRdmaContext {
    id: *mut rdma_cm_id,
    device: RdmaDeviceInfo,
}

impl RealRdmaContext {
    /// Connect to a responder at `ip:port` and receive the `{addr, rkey, len}`
    /// of its RDMA-READable source memory region.
    pub fn connect(ip: &str, port: u16) -> RdmaResult<(Self, u64, u32, usize)> {
        unsafe {
            let node = CString::new(ip).map_err(|_| op_failed("bad ip"))?;
            let svc = CString::new(port.to_string()).unwrap();
            let mut hints: rdma_addrinfo = std::mem::zeroed();
            hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as c_int;
            let mut res: *mut rdma_addrinfo = null_mut();
            if rdma_getaddrinfo(node.as_ptr(), svc.as_ptr(), &hints, &mut res) != 0 {
                return Err(op_failed("rdma_getaddrinfo(client)"));
            }
            let mut attr = qp_init_attr();
            let mut id: *mut rdma_cm_id = null_mut();
            let r = rdma_create_ep(&mut id, res, null_mut(), &mut attr);
            rdma_freeaddrinfo(res);
            if r != 0 {
                return Err(op_failed("rdma_create_ep(client)"));
            }

            // Post a receive for the 16-byte RemoteInfo before connecting.
            let mut meta = RemoteInfo::default();
            let meta_ptr = (&mut meta as *mut RemoteInfo).cast::<c_void>();
            let meta_mr = rdma_reg_msgs(id, meta_ptr, META_LEN);
            if meta_mr.is_null() {
                rdma_destroy_ep(id);
                return Err(op_failed("rdma_reg_msgs(meta)"));
            }
            if rdma_post_recv(id, null_mut(), meta_ptr, META_LEN, meta_mr) != 0 {
                rdma_destroy_ep(id);
                return Err(op_failed("rdma_post_recv(meta)"));
            }

            let mut cp = conn_param();
            if rdma_connect(id, &mut cp) != 0 {
                rdma_destroy_ep(id);
                return Err(op_failed("rdma_connect"));
            }
            wait_comp(rdma_get_recv_comp, id, "recv(meta)")?;
            rdma_dereg_mr(meta_mr);

            let device = query_device(id);
            Ok((
                RealRdmaContext { id, device },
                meta.addr,
                meta.rkey,
                meta.len as usize,
            ))
        }
    }

    /// Register a local buffer (LOCAL_WRITE) as the destination of an RDMA READ.
    /// Returns the engine's `MemoryRegion` plus the raw `ibv_mr` needed to post.
    pub fn register_memory(&self, buf: &mut [u8]) -> RdmaResult<(MemoryRegion, *mut ibv_mr)> {
        unsafe {
            let mr = rdma_reg_msgs(self.id, buf.as_mut_ptr().cast(), buf.len());
            if mr.is_null() {
                return Err(op_failed("rdma_reg_msgs(dest)"));
            }
            Ok((
                MemoryRegion {
                    addr: buf.as_ptr() as u64,
                    rkey: (*mr).rkey,
                    lkey: (*mr).lkey,
                    size: buf.len(),
                    registered: true,
                },
                mr,
            ))
        }
    }

    /// Post a genuine `IBV_WR_RDMA_READ`: pull `buf.len()` bytes from the remote
    /// `{remote_addr, rkey}` into the local buffer described by `mr`.
    pub fn post_read(
        &self,
        buf: &mut [u8],
        mr: *mut ibv_mr,
        remote_addr: u64,
        rkey: u32,
        wr_id: u64,
    ) -> RdmaResult<()> {
        unsafe {
            let flags = ibv_send_flags::IBV_SEND_SIGNALED.0 as c_int;
            if rdma_post_read(
                self.id,
                wr_id as *mut c_void,
                buf.as_mut_ptr().cast(),
                buf.len(),
                mr,
                flags,
                remote_addr,
                rkey,
            ) != 0
            {
                return Err(op_failed("rdma_post_read"));
            }
            Ok(())
        }
    }

    /// Wait for the RDMA READ completion (it lands on the send queue).
    pub fn poll_completion(&self) -> RdmaResult<WorkCompletion> {
        unsafe {
            let wc = wait_comp(rdma_get_send_comp, self.id, "send_comp(read)")?;
            Ok(WorkCompletion {
                wr_id: wc.wr_id,
                status: CompletionStatus::from(wc.status as u32),
                opcode: RdmaOp::Read,
                byte_len: wc.byte_len,
                imm_data: None,
            })
        }
    }

    pub fn device_info(&self) -> &RdmaDeviceInfo {
        &self.device
    }

    /// Signal the responder we are done (1-byte send) so it can tear down, then
    /// disconnect.
    pub fn finish(&self) {
        unsafe {
            let mut done = [1u8; 1];
            let mr = rdma_reg_msgs(self.id, done.as_mut_ptr().cast(), 1);
            if !mr.is_null() {
                if rdma_post_send(self.id, null_mut(), done.as_mut_ptr().cast(), 1, mr, 0) == 0 {
                    let _ = wait_comp(rdma_get_send_comp, self.id, "send_comp(done)");
                }
                rdma_dereg_mr(mr);
            }
            rdma_disconnect(self.id);
        }
    }
}

impl Drop for RealRdmaContext {
    fn drop(&mut self) {
        unsafe {
            if !self.id.is_null() {
                rdma_destroy_ep(self.id);
            }
        }
    }
}

/// A throwaway loopback RDMA-READ target: registers `data` for `REMOTE_READ`,
/// accepts one client, hands it `{addr, rkey, len}`, and serves until the client
/// signals done. Blocking — run on a thread; `data` must outlive the call so the
/// client's reads see real bytes.
pub struct RealResponder;

impl RealResponder {
    pub fn serve_once(
        ip: &str,
        port: u16,
        data: &mut [u8],
        ready: impl FnOnce(),
    ) -> RdmaResult<()> {
        unsafe {
            let node = CString::new(ip).map_err(|_| op_failed("bad ip"))?;
            let svc = CString::new(port.to_string()).unwrap();
            let mut hints: rdma_addrinfo = std::mem::zeroed();
            hints.ai_flags = RAI_PASSIVE as c_int;
            hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as c_int;
            let mut res: *mut rdma_addrinfo = null_mut();
            if rdma_getaddrinfo(node.as_ptr(), svc.as_ptr(), &hints, &mut res) != 0 {
                return Err(op_failed("rdma_getaddrinfo(server)"));
            }
            let mut attr = qp_init_attr();
            let mut listen_id: *mut rdma_cm_id = null_mut();
            let r = rdma_create_ep(&mut listen_id, res, null_mut(), &mut attr);
            rdma_freeaddrinfo(res);
            if r != 0 {
                return Err(op_failed("rdma_create_ep(server)"));
            }
            if rdma_listen(listen_id, 1) != 0 {
                rdma_destroy_ep(listen_id);
                return Err(op_failed("rdma_listen"));
            }
            ready(); // the listener is up; let the client connect

            let mut id: *mut rdma_cm_id = null_mut();
            if rdma_get_request(listen_id, &mut id) != 0 {
                rdma_destroy_ep(listen_id);
                return Err(op_failed("rdma_get_request"));
            }

            // Expose the source buffer for REMOTE_READ.
            let src_mr = rdma_reg_read(id, data.as_mut_ptr().cast(), data.len());
            if src_mr.is_null() {
                rdma_destroy_ep(id);
                rdma_destroy_ep(listen_id);
                return Err(op_failed("rdma_reg_read"));
            }

            let mut meta = RemoteInfo {
                addr: data.as_ptr() as u64,
                rkey: (*src_mr).rkey,
                len: data.len() as u32,
            };
            let meta_ptr = (&mut meta as *mut RemoteInfo).cast::<c_void>();
            let meta_mr = rdma_reg_msgs(id, meta_ptr, META_LEN);
            let mut done = [0u8; 1];
            let done_mr = rdma_reg_msgs(id, done.as_mut_ptr().cast(), 1);
            if meta_mr.is_null() || done_mr.is_null() {
                return Err(op_failed("rdma_reg_msgs(server)"));
            }
            // Post the done-signal receive before accepting.
            if rdma_post_recv(id, null_mut(), done.as_mut_ptr().cast(), 1, done_mr) != 0 {
                return Err(op_failed("rdma_post_recv(done)"));
            }

            let mut cp = conn_param();
            if rdma_accept(id, &mut cp) != 0 {
                return Err(op_failed("rdma_accept"));
            }

            // Hand the client the source MR coordinates.
            if rdma_post_send(id, null_mut(), meta_ptr, META_LEN, meta_mr, 0) != 0 {
                return Err(op_failed("rdma_post_send(meta)"));
            }
            wait_comp(rdma_get_send_comp, id, "send_comp(meta)")?;

            // Keep src_mr/data alive until the client finishes its read.
            wait_comp(rdma_get_recv_comp, id, "recv_comp(done)")?;

            rdma_disconnect(id);
            rdma_dereg_mr(src_mr);
            rdma_dereg_mr(meta_mr);
            rdma_dereg_mr(done_mr);
            rdma_destroy_ep(id);
            rdma_destroy_ep(listen_id);
            Ok(())
        }
    }
}
