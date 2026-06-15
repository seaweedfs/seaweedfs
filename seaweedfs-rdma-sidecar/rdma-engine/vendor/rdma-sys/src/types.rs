use crate::*;

/// This file defines the types directly or indirectly involving union,
/// in that BindGen cannot handle union very well, so mannually define them.

/// Struct types involve union in <infiniband/verbs.h>

// ibv_gid related union and struct types
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ibv_gid_global_t {
    pub subnet_prefix: __be64,
    pub interface_id: __be64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union ibv_gid {
    pub raw: [u8; 16],
    pub global: ibv_gid_global_t,
}

// ibv_async_event related union and struct type
#[repr(C)]
pub union ibv_async_event_element_t {
    pub cq: *mut ibv_cq,
    pub qp: *mut ibv_qp,
    pub srq: *mut ibv_srq,
    pub wq: *mut ibv_wq,
    pub port_num: c_int,
}

#[repr(C)]
pub struct ibv_async_event {
    pub element: ibv_async_event_element_t,
    pub event_type: ibv_event_type,
}

// ibv_wc related union and struct types
#[repr(C)]
pub union imm_data_invalidated_rkey_union_t {
    /// When (wc_flags & IBV_WC_WITH_IMM): Immediate data in network byte order.
    pub imm_data: __be32,
    /// When (wc_flags & IBV_WC_WITH_INV): Stores the invalidated rkey.
    pub invalidated_rkey: u32,
}

#[repr(C)]
pub struct ibv_wc {
    pub wr_id: u64,
    pub status: ibv_wc_status::Type,
    pub opcode: ibv_wc_opcode::Type,
    pub vendor_err: u32,
    pub byte_len: u32,
    pub imm_data_invalidated_rkey_union: imm_data_invalidated_rkey_union_t,
    pub qp_num: u32,
    pub src_qp: u32,
    pub wc_flags: c_uint,
    pub pkey_index: u16,
    pub slid: u16,
    pub sl: u8,
    pub dlid_path_bits: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ibv_global_route {
    pub dgid: ibv_gid,
    pub flow_label: u32,
    pub sgid_index: u8,
    pub hop_limit: u8,
    pub traffic_class: u8,
}

// ibv_send_wr related union and struct types
#[repr(C)]
#[derive(Copy, Clone)]
pub struct ibv_mw_bind_info {
    pub mr: *mut ibv_mr,
    pub addr: u64,
    pub length: u64,
    pub mw_access_flags: ::std::os::raw::c_uint,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct rdma_t {
    pub remote_addr: u64,
    pub rkey: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct atomic_t {
    pub remote_addr: u64,
    pub compare_add: u64,
    pub swap: u64,
    pub rkey: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ud_t {
    pub ah: *mut ibv_ah,
    pub remote_qpn: u32,
    pub remote_qkey: u32,
}

#[repr(C)]
pub union wr_t {
    pub rdma: rdma_t,
    pub atomic: atomic_t,
    pub ud: ud_t,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xrc_t {
    pub remote_srqn: u32,
}

#[repr(C)]
pub union qp_type_t {
    pub xrc: xrc_t,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct bind_mw_t {
    pub mw: *mut ibv_mw,
    pub rkey: u32,
    pub bind_info: ibv_mw_bind_info,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct tso_t {
    pub hdr: *mut c_void,
    pub hdr_sz: u16,
    pub mss: u16,
}

#[repr(C)]
pub union bind_mw_tso_union_t {
    pub bind_mw: bind_mw_t,
    pub tso: tso_t,
}

#[repr(C)]
pub struct ibv_send_wr {
    pub wr_id: u64,
    pub next: *mut Self,
    pub sg_list: *mut ibv_sge,
    pub num_sge: c_int,
    pub opcode: ibv_wr_opcode::Type,
    pub send_flags: c_uint,
    /// When opcode is *_WITH_IMM: Immediate data in network byte order.
    /// When opcode is *_INV: Stores the rkey to invalidate
    pub imm_data_invalidated_rkey_union: imm_data_invalidated_rkey_union_t,
    pub wr: wr_t,
    pub qp_type: qp_type_t,
    pub bind_mw_tso_union: bind_mw_tso_union_t,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct add_t {
    pub recv_wr_id: u64,
    pub sg_list: *mut ibv_sge,
    pub num_sge: c_int,
    pub tag: u64,
    pub mask: u64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct tm_t {
    pub unexpected_cnt: u32,
    pub handle: u32,
    pub add: add_t,
}

#[repr(C)]
pub struct ibv_ops_wr {
    wr_id: u64,
    next: *mut Self,
    opcode: ibv_ops_wr_opcode::Type,
    flags: c_int,
    tm: tm_t,
}

// ibv_flow_spec related union and struct types
#[repr(C)]
#[derive(Clone, Copy)]
pub struct hdr_t {
    pub type_: ibv_flow_spec_type::Type,
    pub size: u16,
}

#[repr(C)]
pub union ibv_flow_spec_union_t {
    pub hdr: hdr_t,
    pub eth: ibv_flow_spec_eth,
    pub ipv4: ibv_flow_spec_ipv4,
    pub tcp_udp: ibv_flow_spec_tcp_udp,
    pub ipv4_ext: ibv_flow_spec_ipv4_ext,
    pub ipv6: ibv_flow_spec_ipv6,
    pub esp: ibv_flow_spec_esp,
    pub tunnel: ibv_flow_spec_tunnel,
    pub gre: ibv_flow_spec_gre,
    pub mpls: ibv_flow_spec_mpls,
    pub flow_tag: ibv_flow_spec_action_tag,
    pub drop: ibv_flow_spec_action_drop,
    pub handle: ibv_flow_spec_action_handle,
    pub flow_count: ibv_flow_spec_counter_action,
}

#[repr(C)]
pub struct ibv_flow_spec {
    pub ibv_flow_spec_union: ibv_flow_spec_union_t,
}

/// Struct types involve union in <rdma/rdma_cma.h>

// rdma_addr related union and struct types
#[repr(C)]
#[derive(Clone, Copy)]
pub struct rdma_ib_addr {
    pub sgid: ibv_gid,
    pub dgid: ibv_gid,
    pub pkey: __be16,
}

#[repr(C)]
pub union src_addr_union_t {
    pub src_addr: libc::sockaddr,
    pub src_sin: libc::sockaddr_in,
    pub src_sin6: libc::sockaddr_in6,
    pub src_storage: libc::sockaddr_storage,
}

#[repr(C)]
pub union dst_addr_union_t {
    pub dst_addr: libc::sockaddr,
    pub dst_sin: libc::sockaddr_in,
    pub dst_sin6: libc::sockaddr_in6,
    pub dst_storage: libc::sockaddr_storage,
}

#[repr(C)]
pub union addr_union_t {
    pub ibaddr: rdma_ib_addr,
}

#[repr(C)]
pub struct rdma_addr {
    pub src_addr_union: src_addr_union_t,
    pub dst_addr_union: dst_addr_union_t,
    pub addr: addr_union_t,
}

/// rdma_cm_event related union and struct types

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ibv_ah_attr {
    pub grh: ibv_global_route,
    pub dlid: u16,
    pub sl: u8,
    pub src_path_bits: u8,
    pub static_rate: u8,
    pub is_global: u8,
    pub port_num: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct rdma_ud_param {
    pub private_data: *const ::std::os::raw::c_void,
    pub private_data_len: u8,
    pub ah_attr: ibv_ah_attr,
    pub qp_num: u32,
    pub qkey: u32,
}

#[repr(C)]
pub union param_t {
    pub conn: rdma_conn_param,
    pub ud: rdma_ud_param,
}

#[repr(C)]
pub struct rdma_cm_event {
    pub id: *mut rdma_cm_id,
    pub listen_id: *mut rdma_cm_id,
    pub event: rdma_cm_event_type::Type,
    pub status: c_int,
    pub param: param_t,
}
