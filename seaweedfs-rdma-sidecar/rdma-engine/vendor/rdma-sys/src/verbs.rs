use crate::*;

use std::mem;
use std::ptr;

/// Inline functions from <infiniband/verbs.h>

pub type ibv_advise_mr_advice = ib_uverbs_advise_mr_advice::Type;

// ibv_qp_ex related inline functions
#[inline]
pub unsafe fn ibv_wr_atomic_cmp_swp(
    qp: *mut ibv_qp_ex,
    rkey: u32,
    remote_addr: u64,
    compare: u64,
    swap: u64,
) {
    (*qp).wr_atomic_cmp_swp.unwrap()(qp, rkey, remote_addr, compare, swap);
}

#[inline]
pub unsafe fn ibv_wr_atomic_fetch_add(qp: *mut ibv_qp_ex, rkey: u32, remote_addr: u64, add: u64) {
    (*qp).wr_atomic_fetch_add.unwrap()(qp, rkey, remote_addr, add);
}

#[inline]
pub unsafe fn ibv_wr_bind_mw(
    qp: *mut ibv_qp_ex,
    mw: *mut ibv_mw,
    rkey: u32,
    bind_info: *const ibv_mw_bind_info,
) {
    (*qp).wr_bind_mw.unwrap()(qp, mw, rkey, bind_info);
}

#[inline]
pub unsafe fn ibv_wr_local_inv(qp: *mut ibv_qp_ex, invalidate_rkey: u32) {
    (*qp).wr_local_inv.unwrap()(qp, invalidate_rkey);
}

#[inline]
pub unsafe fn ibv_wr_rdma_read(qp: *mut ibv_qp_ex, rkey: u32, remote_addr: u64) {
    (*qp).wr_rdma_read.unwrap()(qp, rkey, remote_addr);
}

#[inline]
pub unsafe fn ibv_wr_rdma_write(qp: *mut ibv_qp_ex, rkey: u32, remote_addr: u64) {
    (*qp).wr_rdma_write.unwrap()(qp, rkey, remote_addr);
}

#[inline]
pub unsafe fn ibv_wr_rdma_write_imm(
    qp: *mut ibv_qp_ex,
    rkey: u32,
    remote_addr: u64,
    imm_data: __be32,
) {
    (*qp).wr_rdma_write_imm.unwrap()(qp, rkey, remote_addr, imm_data);
}

#[inline]
pub unsafe fn ibv_wr_send(qp: *mut ibv_qp_ex) {
    (*qp).wr_send.unwrap()(qp);
}

#[inline]
pub unsafe fn ibv_wr_send_imm(qp: *mut ibv_qp_ex, imm_data: __be32) {
    (*qp).wr_send_imm.unwrap()(qp, imm_data);
}

#[inline]
pub unsafe fn ibv_wr_send_inv(qp: *mut ibv_qp_ex, invalidate_rkey: u32) {
    (*qp).wr_send_inv.unwrap()(qp, invalidate_rkey);
}

#[inline]
pub unsafe fn ibv_wr_send_tso(qp: *mut ibv_qp_ex, hdr: *mut c_void, hdr_sz: u16, mss: u16) {
    (*qp).wr_send_tso.unwrap()(qp, hdr, hdr_sz, mss);
}

#[inline]
pub unsafe fn ibv_wr_set_ud_addr(
    qp: *mut ibv_qp_ex,
    ah: *mut ibv_ah,
    remote_qpn: u32,
    remote_qkey: u32,
) {
    (*qp).wr_set_ud_addr.unwrap()(qp, ah, remote_qpn, remote_qkey);
}

#[inline]
pub unsafe fn ibv_wr_set_xrc_srqn(qp: *mut ibv_qp_ex, remote_srqn: u32) {
    (*qp).wr_set_xrc_srqn.unwrap()(qp, remote_srqn);
}

#[inline]
pub unsafe fn ibv_wr_set_inline_data(qp: *mut ibv_qp_ex, addr: *mut c_void, length: usize) {
    (*qp).wr_set_inline_data.unwrap()(qp, addr, length);
}

#[inline]
pub unsafe fn ibv_wr_set_inline_data_list(
    qp: *mut ibv_qp_ex,
    num_buf: usize,
    buf_list: *const ibv_data_buf,
) {
    (*qp).wr_set_inline_data_list.unwrap()(qp, num_buf, buf_list);
}

#[inline]
pub unsafe fn ibv_wr_set_sge(qp: *mut ibv_qp_ex, lkey: u32, addr: u64, length: u32) {
    (*qp).wr_set_sge.unwrap()(qp, lkey, addr, length);
}

#[inline]
pub unsafe fn ibv_wr_set_sge_list(qp: *mut ibv_qp_ex, num_sge: usize, sg_list: *const ibv_sge) {
    (*qp).wr_set_sge_list.unwrap()(qp, num_sge, sg_list);
}

#[inline]
pub unsafe fn ibv_wr_start(qp: *mut ibv_qp_ex) {
    (*qp).wr_start.unwrap()(qp);
}

#[inline]
pub unsafe fn ibv_wr_complete(qp: *mut ibv_qp_ex) -> c_int {
    (*qp).wr_complete.unwrap()(qp)
}

#[inline]
pub unsafe fn ibv_wr_abort(qp: *mut ibv_qp_ex) {
    (*qp).wr_abort.unwrap()(qp)
}

// ibv_cq_ex related inline functions
#[inline]
pub unsafe fn ibv_cq_ex_to_cq(cq: *mut ibv_cq_ex) -> *mut ibv_cq {
    cq as *mut ibv_cq_ex as *mut ibv_cq
}

#[inline]
pub unsafe fn ibv_start_poll(cq: *mut ibv_cq_ex, attr: *mut ibv_poll_cq_attr) -> c_int {
    (*cq).start_poll.unwrap()(cq, attr)
}

#[inline]
pub unsafe fn ibv_next_poll(cq: *mut ibv_cq_ex) -> c_int {
    (*cq).next_poll.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_end_poll(cq: *mut ibv_cq_ex) {
    (*cq).end_poll.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_opcode(cq: *mut ibv_cq_ex) -> ibv_wc_opcode::Type {
    (*cq).read_opcode.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_vendor_err(cq: *mut ibv_cq_ex) -> u32 {
    (*cq).read_vendor_err.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_byte_len(cq: *mut ibv_cq_ex) -> u32 {
    (*cq).read_byte_len.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_imm_data(cq: *mut ibv_cq_ex) -> __be32 {
    (*cq).read_imm_data.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_invalidated_rkey(cq: *mut ibv_cq_ex) -> u32 {
    // #ifdef __CHECKER__
    //     return (__attribute__((force)) uint32_t)cq->read_imm_data(cq);
    // #else
    //     return cq->read_imm_data(cq);
    // #endif
    (*cq).read_imm_data.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_qp_num(cq: *mut ibv_cq_ex) -> u32 {
    (*cq).read_qp_num.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_src_qp(cq: *mut ibv_cq_ex) -> u32 {
    (*cq).read_src_qp.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_wc_flags(cq: *mut ibv_cq_ex) -> c_uint {
    (*cq).read_wc_flags.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_slid(cq: *mut ibv_cq_ex) -> u32 {
    (*cq).read_slid.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_sl(cq: *mut ibv_cq_ex) -> u8 {
    (*cq).read_sl.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_dlid_path_bits(cq: *mut ibv_cq_ex) -> u8 {
    (*cq).read_dlid_path_bits.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_completion_ts(cq: *mut ibv_cq_ex) -> u64 {
    (*cq).read_completion_ts.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_completion_wallclock_ns(cq: *mut ibv_cq_ex) -> u64 {
    (*cq).read_completion_wallclock_ns.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_cvlan(cq: *mut ibv_cq_ex) -> u16 {
    (*cq).read_cvlan.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_flow_tag(cq: *mut ibv_cq_ex) -> u32 {
    (*cq).read_flow_tag.unwrap()(cq)
}

#[inline]
pub unsafe fn ibv_wc_read_tm_info(cq: *mut ibv_cq_ex, tm_info: *mut ibv_wc_tm_info) {
    (*cq).read_tm_info.unwrap()(cq, tm_info)
}

// ibv_wq related inline function
#[inline]
pub unsafe fn ibv_post_wq_recv(
    wq: *mut ibv_wq,
    recv_wr: *mut ibv_recv_wr,
    bad_recv_wr: *mut *mut ibv_recv_wr,
) -> c_int {
    (*wq).post_recv.unwrap()(wq, recv_wr, bad_recv_wr)
}

// Use intrusive_collections::container_of! instread, once it's stable not nightly
macro_rules! container_of {
    ($ptr:expr, $container:path, $field:ident) => {{
        ($ptr as *const _ as *const u8).sub(memoffset::offset_of!($container, $field))
            as *const $container
    }};
}

// Utility function to get verbs_context from ibv_context
#[inline]
unsafe fn verbs_get_ctx(ctx: *const ibv_context) -> Option<*mut verbs_context> {
    if (*ctx).abi_compat as usize != u32::MAX as usize {
        None
    } else {
        let vcp = container_of!(ctx, verbs_context, context) as *mut _;
        Some(vcp)
    }
}

macro_rules! verbs_get_ctx_op {
    ($vcr:expr, $field:ident) => {
        if let Some(vc) = verbs_get_ctx($vcr) {
            if (*vc).sz < mem::size_of_val(&*vc) - memoffset::offset_of!(verbs_context, $field) {
                None
            } else {
                if (*vc).$field.is_some() {
                    Some(vc)
                } else {
                    None
                }
            }
        } else {
            None
        }
    };
}

// TODO: note that ibv_query_port, ibv_get_device_list, ibv_reg_mr, and
// ibv_reg_mr_iova are redefined using ___ibv_query_port,
// __ibv_get_device_list, __ibv_reg_mr, and __ibv_reg_mr_iova in C, which
// should be handled properly in Rust.

// When statically linking the user can set RDMA_STATIC_PROVIDERS to a comma
// separated list of provider names to include in the static link, and this
// machinery will cause those providers to be included statically.
//
// Linking will fail if this is set for dynamic linking.
//
// #define ibv_get_device_list(num_devices) __ibv_get_device_list(num_devices)
// #endif
// #ifdef RDMA_STATIC_PROVIDERS
// #define _RDMA_STATIC_PREFIX_(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11,     \
//                              _12, _13, _14, _15, _16, _17, ...)                \
//         &verbs_provider_##_1, &verbs_provider_##_2, &verbs_provider_##_3,      \
//                 &verbs_provider_##_4, &verbs_provider_##_5,                    \
//                 &verbs_provider_##_6, &verbs_provider_##_7,                    \
//                 &verbs_provider_##_8, &verbs_provider_##_9,                    \
//                 &verbs_provider_##_10, &verbs_provider_##_11,                  \
//                 &verbs_provider_##_12, &verbs_provider_##_13,                  \
//                 &verbs_provider_##_14, &verbs_provider_##_15,                  \
//                 &verbs_provider_##_16, &verbs_provider_##_17
// #define _RDMA_STATIC_PREFIX(arg)                                               \
//         _RDMA_STATIC_PREFIX_(arg, none, none, none, none, none, none, none,    \
//                              none, none, none, none, none, none, none, none,   \
//                              none)
pub unsafe fn __ibv_get_device_list(num_devices: *mut c_int) -> *mut *mut ibv_device {
    // TODO: check static linking compatibility
    // ibv_static_providers(NULL, _RDMA_STATIC_PREFIX(RDMA_STATIC_PROVIDERS), NULL);
    ibv_get_device_list(num_devices)
}

// TODO: missing variable args function
// void ibv_static_providers(void *unused, ...);

// ibv_context related inline function
#[inline]
pub unsafe fn ___ibv_query_port(
    context: *mut ibv_context,
    port_num: u8,
    port_attr: *mut ibv_port_attr,
) -> c_int {
    let vcr = verbs_get_ctx_op!(context, query_port);

    if let Some(vctx) = vcr {
        (*vctx).query_port.unwrap()(context, port_num, port_attr, mem::size_of_val(&*port_attr))
    } else {
        // TODO: memset(port_attr, 0, sizeof(*port_attr));
        let compat_attr = port_attr as *mut _ as *mut _compat_ibv_port_attr;
        ibv_query_port(context, port_num, compat_attr)
    }
}

// ibv_flow related inline functions
#[inline]
pub unsafe fn ibv_create_flow(qp: *mut ibv_qp, flow: *mut ibv_flow_attr) -> Option<*mut ibv_flow> {
    let vcr = verbs_get_ctx_op!((*qp).context, ibv_create_flow);

    if let Some(vctx) = vcr {
        Some((*vctx).ibv_create_flow.unwrap()(qp, flow))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_destroy_flow(flow_id: *mut ibv_flow) -> c_int {
    let vcr = verbs_get_ctx_op!((*flow_id).context, ibv_destroy_flow);

    if let Some(vctx) = vcr {
        (*vctx).ibv_destroy_flow.unwrap()(flow_id)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_create_flow_action_esp(
    ctx: *mut ibv_context,
    esp: *mut ibv_flow_action_esp_attr,
) -> Option<*mut ibv_flow_action> {
    let vcr = verbs_get_ctx_op!(ctx, create_flow_action_esp);

    if let Some(vctx) = vcr {
        Some((*vctx).create_flow_action_esp.unwrap()(ctx, esp))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_modify_flow_action_esp(
    action: *mut ibv_flow_action,
    esp: *mut ibv_flow_action_esp_attr,
) -> c_int {
    let vcr = verbs_get_ctx_op!((*action).context, modify_flow_action_esp);

    if let Some(vctx) = vcr {
        (*vctx).modify_flow_action_esp.unwrap()(action, esp)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_destroy_flow_action(action: *mut ibv_flow_action) -> c_int {
    let vcr = verbs_get_ctx_op!((*action).context, destroy_flow_action);

    if let Some(vctx) = vcr {
        (*vctx).destroy_flow_action.unwrap()(action)
    } else {
        libc::EOPNOTSUPP
    }
}

// ibv_xrcd related inline functions
#[inline]
pub unsafe fn ibv_open_xrcd(
    context: *mut ibv_context,
    xrcd_init_attr: *mut ibv_xrcd_init_attr,
) -> Option<*mut ibv_xrcd> {
    let vcr = verbs_get_ctx_op!(context, open_xrcd);

    if let Some(vctx) = vcr {
        Some((*vctx).open_xrcd.unwrap()(context, xrcd_init_attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_close_xrcd(xrcd: *mut ibv_xrcd) -> c_int {
    let vctx = verbs_get_ctx((*xrcd).context);

    (*vctx.unwrap()).close_xrcd.unwrap()(xrcd)
}

// use new ibv_reg_mr version only if access flags that require it are used
#[inline]
pub unsafe fn __ibv_reg_mr(
    pd: *mut ibv_pd,
    addr: *mut c_void,
    length: usize,
    access: c_uint,
    is_access_const: c_int,
) -> *mut ibv_mr {
    if is_access_const != 0
        && (ib_uverbs_access_flags(access)
            & ib_uverbs_access_flags::IB_UVERBS_ACCESS_OPTIONAL_RANGE)
            == ib_uverbs_access_flags(0)
    {
        ibv_reg_mr(pd, addr, length, access as c_int)
    } else {
        ibv_reg_mr_iova2(pd, addr, length, addr as u64, access)
    }
}
// TODO: handle C macro defined function
// #define ibv_reg_mr(pd, addr, length, access) \
//     __ibv_reg_mr(pd, addr, length, access,      \
//              __builtin_constant_p(              \
//                  ((access) & IBV_ACCESS_OPTIONAL_RANGE) == 0))

// use new ibv_reg_mr version only if access flags that require it are used
#[inline]
pub unsafe fn __ibv_reg_mr_iova(
    pd: *mut ibv_pd,
    addr: *mut c_void,
    length: usize,
    iova: u64,
    access: c_uint,
    is_access_const: c_int,
) -> *mut ibv_mr {
    if is_access_const != 0
        && (ib_uverbs_access_flags(access)
            & ib_uverbs_access_flags::IB_UVERBS_ACCESS_OPTIONAL_RANGE)
            == ib_uverbs_access_flags(0)
    {
        ibv_reg_mr_iova(pd, addr, length, iova, access as c_int)
    } else {
        ibv_reg_mr_iova2(pd, addr, length, iova, access)
    }
}
// TODO: handle C macro defined function
// #define ibv_reg_mr_iova(pd, addr, length, iova, access)                        \
//     __ibv_reg_mr_iova(pd, addr, length, iova, access,                      \
//               __builtin_constant_p(                                \
//                   ((access) & IBV_ACCESS_OPTIONAL_RANGE) == 0))

// ibv_mw related inline functions
#[inline]
pub unsafe fn ibv_alloc_mw(pd: *mut ibv_pd, type_: ibv_mw_type::Type) -> Option<*mut ibv_mw> {
    if (*(*pd).context).ops.alloc_mw.is_some() {
        Some((*(*pd).context).ops.alloc_mw.unwrap()(pd, type_))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_dealloc_mw(mw: *mut ibv_mw) -> c_int {
    (*(*mw).context).ops.dealloc_mw.unwrap()(mw)
}

// ibv_inc_rkey - Increase the 8 lsb in the given rkey
#[inline]
pub unsafe fn ibv_inc_rkey(rkey: u32) -> u32 {
    let mask: u32 = 0x000000ff;
    let newtag = ((rkey + 1) & mask) as u8;

    (rkey & !mask) | (newtag as u32)
}

#[inline]
pub unsafe fn ibv_bind_mw(qp: *mut ibv_qp, mw: *mut ibv_mw, mw_bind: *mut ibv_mw_bind) -> c_int {
    if (*mw).type_ != ibv_mw_type::IBV_MW_TYPE_1 {
        libc::EINVAL
    } else {
        (*(*mw).context).ops.bind_mw.unwrap()(qp, mw, mw_bind)
    }
}

#[inline]
pub unsafe fn ibv_advise_mr(
    pd: *mut ibv_pd,
    advice: ibv_advise_mr_advice,
    flags: u32,
    sg_list: *mut ibv_sge,
    num_sge: u32,
) -> c_int {
    let vcr = verbs_get_ctx_op!((*pd).context, advise_mr);

    if let Some(vctx) = vcr {
        (*vctx).advise_mr.unwrap()(pd, advice, flags, sg_list, num_sge)
    } else {
        libc::EOPNOTSUPP
    }
}

// ibv_dm related inline functions
#[inline]
pub unsafe fn ibv_alloc_dm(
    context: *mut ibv_context,
    attr: *mut ibv_alloc_dm_attr,
) -> Option<*mut ibv_dm> {
    let vcr = verbs_get_ctx_op!(context, alloc_dm);

    if let Some(vctx) = vcr {
        Some((*vctx).alloc_dm.unwrap()(context, attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_free_dm(dm: *mut ibv_dm) -> c_int {
    let vcr = verbs_get_ctx_op!((*dm).context, free_dm);

    if let Some(vctx) = vcr {
        (*vctx).free_dm.unwrap()(dm)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_memcpy_to_dm(
    dm: *mut ibv_dm,
    dm_offset: u64,
    host_addr: *const c_void,
    length: usize,
) -> c_int {
    (*dm).memcpy_to_dm.unwrap()(dm, dm_offset, host_addr, length)
}

#[inline]
pub unsafe fn ibv_memcpy_from_dm(
    host_addr: *mut c_void,
    dm: *mut ibv_dm,
    dm_offset: u64,
    length: usize,
) -> c_int {
    (*dm).memcpy_from_dm.unwrap()(host_addr, dm, dm_offset, length)
}

#[inline]
pub unsafe fn ibv_alloc_null_mr(pd: *mut ibv_pd) -> Option<*mut ibv_mr> {
    let vcr = verbs_get_ctx_op!((*pd).context, alloc_null_mr);

    if let Some(vctx) = vcr {
        Some((*vctx).alloc_null_mr.unwrap()(pd))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_reg_dm_mr(
    pd: *mut ibv_pd,
    dm: *mut ibv_dm,
    dm_offset: u64,
    length: usize,
    access: u32,
) -> Option<*mut ibv_mr> {
    let vcr = verbs_get_ctx_op!((*pd).context, reg_dm_mr);

    if let Some(vctx) = vcr {
        Some((*vctx).reg_dm_mr.unwrap()(
            pd, dm, dm_offset, length, access,
        ))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

// ibv_cq_ex related inline function
#[inline]
pub unsafe fn ibv_create_cq_ex(
    context: *mut ibv_context,
    cq_attr: *mut ibv_cq_init_attr_ex,
) -> Option<*mut ibv_cq_ex> {
    let vcr = verbs_get_ctx_op!(context, create_cq_ex);

    if let Some(vctx) = vcr {
        Some((*vctx).create_cq_ex.unwrap()(context, cq_attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

// ibv_cq related inline functions
#[inline]
pub unsafe fn ibv_poll_cq(cq: *mut ibv_cq, num_entries: i32, wc: *mut ibv_wc) -> c_int {
    (*(*cq).context).ops.poll_cq.unwrap()(cq, num_entries, wc)
}

#[inline]
pub unsafe fn ibv_req_notify_cq(cq: *mut ibv_cq, solicited_only: i32) -> c_int {
    (*(*cq).context).ops.req_notify_cq.unwrap()(cq, solicited_only)
}

#[inline]
pub unsafe fn ibv_modify_cq(cq: *mut ibv_cq, attr: *mut ibv_modify_cq_attr) -> c_int {
    let vcr = verbs_get_ctx_op!((*cq).context, modify_cq);

    if let Some(vctx) = vcr {
        (*vctx).modify_cq.unwrap()(cq, attr)
    } else {
        libc::EOPNOTSUPP
    }
}

// ibv_srq related inline functions
#[inline]
pub unsafe fn ibv_create_srq_ex(
    context: *mut ibv_context,
    srq_init_attr_ex: *mut ibv_srq_init_attr_ex,
) -> Option<*mut ibv_srq> {
    let mask = ibv_srq_init_attr_mask((*srq_init_attr_ex).comp_mask);
    let mask_inv = ibv_srq_init_attr_mask(!(*srq_init_attr_ex).comp_mask);
    let zero = ibv_srq_init_attr_mask(0);

    // TODO: verify the condition
    let cond = (mask_inv
        | (ibv_srq_init_attr_mask::IBV_SRQ_INIT_ATTR_PD
            & ibv_srq_init_attr_mask::IBV_SRQ_INIT_ATTR_TYPE))
        != zero
        && (mask & ibv_srq_init_attr_mask::IBV_SRQ_INIT_ATTR_PD) != zero
        && ((mask & ibv_srq_init_attr_mask::IBV_SRQ_INIT_ATTR_TYPE) != zero
            || ((*srq_init_attr_ex).srq_type == ibv_srq_type::IBV_SRQT_BASIC));
    if cond {
        Some(ibv_create_srq(
            (*srq_init_attr_ex).pd,
            srq_init_attr_ex as *mut ibv_srq_init_attr,
        ))
    } else {
        let vcr = verbs_get_ctx_op!(context, create_srq_ex);

        if let Some(vctx) = vcr {
            Some((*vctx).create_srq_ex.unwrap()(context, srq_init_attr_ex))
        } else {
            *libc::__errno_location() = libc::EOPNOTSUPP;
            None
        }
    }
}

#[inline]
pub unsafe fn ibv_get_srq_num(srq: *mut ibv_srq, srq_num: *mut u32) -> c_int {
    let vcr = verbs_get_ctx_op!((*srq).context, get_srq_num);

    if let Some(vctx) = vcr {
        (*vctx).get_srq_num.unwrap()(srq, srq_num)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_post_srq_recv(
    srq: *mut ibv_srq,
    recv_wr: *mut ibv_recv_wr,
    bad_recv_wr: *mut *mut ibv_recv_wr,
) -> c_int {
    (*(*srq).context).ops.post_srq_recv.unwrap()(srq, recv_wr, bad_recv_wr)
}

#[inline]
pub unsafe fn ibv_post_srq_ops(
    srq: *mut ibv_srq,
    op: *mut ibv_ops_wr,
    bad_op: *mut *mut ibv_ops_wr,
) -> c_int {
    let vcr = verbs_get_ctx_op!((*srq).context, post_srq_ops);

    if let Some(vctx) = vcr {
        (*vctx).post_srq_ops.unwrap()(srq, op, bad_op)
    } else {
        *bad_op = op;
        libc::EOPNOTSUPP
    }
}

// ibv_qp related inline functions
#[inline]
pub unsafe fn ibv_create_qp_ex(
    context: *mut ibv_context,
    qp_init_attr_ex: *mut ibv_qp_init_attr_ex,
) -> Option<*mut ibv_qp> {
    let mask = ibv_qp_init_attr_mask((*qp_init_attr_ex).comp_mask);

    if mask == ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD {
        Some(ibv_create_qp(
            (*qp_init_attr_ex).pd,
            qp_init_attr_ex as *mut ibv_qp_init_attr,
        ))
    } else {
        let vcr = verbs_get_ctx_op!(context, create_qp_ex);

        if let Some(vctx) = vcr {
            Some((*vctx).create_qp_ex.unwrap()(context, qp_init_attr_ex))
        } else {
            *libc::__errno_location() = libc::EOPNOTSUPP;
            None
        }
    }
}

// ibv_td related inline functions
#[inline]
pub unsafe fn ibv_alloc_td(
    context: *mut ibv_context,
    init_attr: *mut ibv_td_init_attr,
) -> Option<*mut ibv_td> {
    let vcr = verbs_get_ctx_op!(context, alloc_td);

    if let Some(vctx) = vcr {
        Some((*vctx).alloc_td.unwrap()(context, init_attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_dealloc_td(td: *mut ibv_td) -> c_int {
    let vcr = verbs_get_ctx_op!((*td).context, dealloc_td);

    if let Some(vctx) = vcr {
        (*vctx).dealloc_td.unwrap()(td)
    } else {
        libc::EOPNOTSUPP
    }
}

// ibv_pd related inline function
#[inline]
pub unsafe fn ibv_alloc_parent_domain(
    context: *mut ibv_context,
    attr: *mut ibv_parent_domain_init_attr,
) -> Option<*mut ibv_pd> {
    let vcr = verbs_get_ctx_op!(context, alloc_parent_domain);

    if let Some(vctx) = vcr {
        Some((*vctx).alloc_parent_domain.unwrap()(context, attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

// device related inline functions
#[inline]
pub unsafe fn ibv_query_rt_values_ex(
    context: *mut ibv_context,
    values: *mut ibv_values_ex,
) -> c_int {
    let vcr = verbs_get_ctx_op!(context, query_rt_values);

    if let Some(vctx) = vcr {
        (*vctx).query_rt_values.unwrap()(context, values)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_query_device_ex(
    context: *mut ibv_context,
    input: *const ibv_query_device_ex_input,
    attr: *mut ibv_device_attr_ex,
) -> c_int {
    let vcr = verbs_get_ctx_op!(context, query_device_ex);

    if let Some(vctx) = vcr {
        let ret = (*vctx).query_device_ex.unwrap()(context, input, attr, mem::size_of_val(&*attr));
        if ret != libc::EOPNOTSUPP {
            return ret;
        }
    }

    // TODO: memset(attr, 0, sizeof(*attr));
    ibv_query_device(context, &mut (*attr).orig_attr)
}

// ibv_qp related inline functions
#[inline]
pub unsafe fn ibv_open_qp(
    context: *mut ibv_context,
    qp_open_attr: *mut ibv_qp_open_attr,
) -> Option<*mut ibv_qp> {
    let vcr = verbs_get_ctx_op!(context, open_qp);

    if let Some(vctx) = vcr {
        Some((*vctx).open_qp.unwrap()(context, qp_open_attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_modify_qp_rate_limit(
    qp: *mut ibv_qp,
    attr: *mut ibv_qp_rate_limit_attr,
) -> c_int {
    let vcr = verbs_get_ctx_op!((*qp).context, modify_qp_rate_limit);

    if let Some(vctx) = vcr {
        (*vctx).modify_qp_rate_limit.unwrap()(qp, attr)
    } else {
        libc::EOPNOTSUPP
    }
}

// ibv_wq related inline functions
#[inline]
pub unsafe fn ibv_create_wq(
    context: *mut ibv_context,
    wq_init_attr: *mut ibv_wq_init_attr,
) -> Option<*mut ibv_wq> {
    let vcr = verbs_get_ctx_op!(context, create_wq);

    if let Some(vctx) = vcr {
        let wq = (*vctx).create_wq.unwrap()(context, wq_init_attr);
        if wq != (ptr::null::<ibv_wq>() as *mut _) {
            (*wq).events_completed = 0;
            libc::pthread_mutex_init(
                &mut (*wq).mutex,
                ptr::null::<libc::pthread_mutexattr_t>() as *mut _,
            );
            libc::pthread_cond_init(
                &mut (*wq).cond,
                ptr::null::<libc::pthread_condattr_t>() as *mut _,
            );
        }
        Some(wq)
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_modify_wq(wq: *mut ibv_wq, wq_attr: *mut ibv_wq_attr) -> c_int {
    let vcr = verbs_get_ctx_op!((*wq).context, modify_wq);

    if let Some(vctx) = vcr {
        (*vctx).modify_wq.unwrap()(wq, wq_attr)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_destroy_wq(wq: *mut ibv_wq) -> c_int {
    let vcr = verbs_get_ctx_op!((*wq).context, destroy_wq);

    if let Some(vctx) = vcr {
        (*vctx).destroy_wq.unwrap()(wq)
    } else {
        libc::EOPNOTSUPP
    }
}

// ibv_rwq_ind_table related inline functions
#[inline]
pub unsafe fn ibv_create_rwq_ind_table(
    context: *mut ibv_context,
    init_attr: *mut ibv_rwq_ind_table_init_attr,
) -> Option<*mut ibv_rwq_ind_table> {
    let vcr = verbs_get_ctx_op!(context, create_rwq_ind_table);

    if let Some(vctx) = vcr {
        Some((*vctx).create_rwq_ind_table.unwrap()(context, init_attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_destroy_rwq_ind_table(rwq_ind_table: *mut ibv_rwq_ind_table) -> c_int {
    let vcr = verbs_get_ctx_op!((*rwq_ind_table).context, destroy_rwq_ind_table);

    if let Some(vctx) = vcr {
        (*vctx).destroy_rwq_ind_table.unwrap()(rwq_ind_table)
    } else {
        libc::EOPNOTSUPP
    }
}

// If IBV_SEND_INLINE flag is set, the data buffers can be reused
// immediately after the call returns.
#[inline]
pub unsafe fn ibv_post_send(
    qp: *mut ibv_qp,
    wr: *mut ibv_send_wr,
    bad_wr: *mut *mut ibv_send_wr,
) -> c_int {
    (*(*qp).context).ops.post_send.unwrap()(qp, wr, bad_wr)
}

#[inline]
pub unsafe fn ibv_post_recv(
    qp: *mut ibv_qp,
    wr: *mut ibv_recv_wr,
    bad_wr: *mut *mut ibv_recv_wr,
) -> c_int {
    (*(*qp).context).ops.post_recv.unwrap()(qp, wr, bad_wr)
}

#[inline]
pub unsafe fn ibv_is_qpt_supported(caps: u32, qpt: ibv_qp_type::Type) -> c_int {
    !!(caps & (1 << qpt)) as c_int
}

// ibv_counters related inline functions
#[inline]
pub unsafe fn ibv_create_counters(
    context: *mut ibv_context,
    init_attr: *mut ibv_counters_init_attr,
) -> Option<*mut ibv_counters> {
    let vcr = verbs_get_ctx_op!(context, create_counters);

    if let Some(vctx) = vcr {
        Some((*vctx).create_counters.unwrap()(context, init_attr))
    } else {
        *libc::__errno_location() = libc::EOPNOTSUPP;
        None
    }
}

#[inline]
pub unsafe fn ibv_destroy_counters(counters: *mut ibv_counters) -> c_int {
    let vcr = verbs_get_ctx_op!((*counters).context, destroy_counters);

    if let Some(vctx) = vcr {
        (*vctx).destroy_counters.unwrap()(counters)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_attach_counters_point_flow(
    counters: *mut ibv_counters,
    attr: *mut ibv_counter_attach_attr,
    flow: *mut ibv_flow,
) -> c_int {
    let vcr = verbs_get_ctx_op!((*counters).context, attach_counters_point_flow);

    if let Some(vctx) = vcr {
        (*vctx).attach_counters_point_flow.unwrap()(counters, attr, flow)
    } else {
        libc::EOPNOTSUPP
    }
}

#[inline]
pub unsafe fn ibv_read_counters(
    counters: *mut ibv_counters,
    counters_value: *mut u64,
    ncounters: u32,
    flags: u32,
) -> c_int {
    let vcr = verbs_get_ctx_op!((*counters).context, read_counters);

    if let Some(vctx) = vcr {
        (*vctx).read_counters.unwrap()(counters, counters_value, ncounters, flags)
    } else {
        libc::EOPNOTSUPP
    }
}

/// Inline functions from <rdma/rdma_cma.h>

pub const RDMA_IB_IP_PS_MASK: u64 = 0xFFFFFFFFFFFF0000;
pub const RDMA_IB_IP_PORT_MASK: u64 = 0x000000000000FFFF;
pub const RDMA_IB_IP_PS_TCP: u64 = 0x0000000001060000;
pub const RDMA_IB_IP_PS_UDP: u64 = 0x0000000001110000;
pub const RDMA_IB_PS_IB: u64 = 0x00000000013F0000;

pub const RDMA_UDP_QKEY: u32 = 0x01234567;

pub const RAI_PASSIVE: u32 = 0x00000001;
pub const RAI_NUMERICHOST: u32 = 0x00000002;
pub const RAI_NOROUTE: u32 = 0x00000004;
pub const RAI_FAMILY: u32 = 0x00000008;

#[inline]
pub unsafe fn rdma_get_local_addr(id: &rdma_cm_id) -> &libc::sockaddr {
    &id.route.addr.src_addr_union.src_addr
}

#[inline]
pub unsafe fn rdma_get_peer_addr(id: &rdma_cm_id) -> &libc::sockaddr {
    &id.route.addr.dst_addr_union.dst_addr
}

/// Inline functions from <rdma/rdma_verbs.h>

#[inline]
pub unsafe fn rdma_seterrno(ret: c_int) -> c_int {
    if ret != 0 {
        *libc::__errno_location() = ret;
        -1
    } else {
        ret
    }
}

#[inline]
pub unsafe fn rdma_reg_msgs(id: *mut rdma_cm_id, addr: *mut c_void, length: usize) -> *mut ibv_mr {
    ibv_reg_mr(
        (*id).pd,
        addr,
        length,
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as c_int,
    )
}

#[inline]
pub unsafe fn rdma_reg_read(id: *mut rdma_cm_id, addr: *mut c_void, length: usize) -> *mut ibv_mr {
    ibv_reg_mr(
        (*id).pd,
        addr,
        length,
        (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ).0
            as c_int,
    )
}

#[inline]
pub unsafe fn rdma_reg_write(id: *mut rdma_cm_id, addr: *mut c_void, length: usize) -> *mut ibv_mr {
    ibv_reg_mr(
        (*id).pd,
        addr,
        length,
        (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE).0
            as c_int,
    )
}

#[inline]
pub unsafe fn rdma_dereg_mr(mr: *mut ibv_mr) -> c_int {
    rdma_seterrno(ibv_dereg_mr(mr))
}

#[inline]
pub unsafe fn rdma_post_recvv(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    sgl: *mut ibv_sge,
    nsge: c_int,
) -> c_int {
    let mut wr = ibv_recv_wr {
        wr_id: context as u64,
        next: ptr::null::<ibv_recv_wr>() as *mut _,
        sg_list: sgl,
        num_sge: nsge,
    };
    let mut bad = ptr::null::<ibv_recv_wr>() as *mut _;

    if (*id).srq as usize != 0 {
        rdma_seterrno(ibv_post_srq_recv((*id).srq, &mut wr, &mut bad))
    } else {
        rdma_seterrno(ibv_post_recv((*id).qp, &mut wr, &mut bad))
    }
}

#[inline]
pub unsafe fn rdma_post_sendv(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    sgl: *mut ibv_sge,
    nsge: c_int,
    flags: c_int,
) -> c_int {
    let mut wr = std::mem::zeroed::<ibv_send_wr>();
    wr.wr_id = context as u64;
    wr.next = ptr::null::<ibv_send_wr>() as *mut _;
    wr.sg_list = sgl;
    wr.num_sge = nsge;
    wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
    wr.send_flags = flags as c_uint;
    let mut bad = ptr::null::<ibv_send_wr>() as *mut _;

    rdma_seterrno(ibv_post_send((*id).qp, &mut wr, &mut bad))
}

#[inline]
pub unsafe fn rdma_post_readv(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    sgl: *mut ibv_sge,
    nsge: c_int,
    flags: c_int,
    remote_addr: u64,
    rkey: u32,
) -> c_int {
    let mut wr = std::mem::zeroed::<ibv_send_wr>();
    wr.wr_id = context as u64;
    wr.next = ptr::null::<ibv_send_wr>() as *mut _;
    wr.sg_list = sgl;
    wr.num_sge = nsge;
    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_READ;
    wr.send_flags = flags as c_uint;
    wr.wr = wr_t {
        rdma: rdma_t { remote_addr, rkey },
    };
    let mut bad = ptr::null::<ibv_send_wr>() as *mut _;

    rdma_seterrno(ibv_post_send((*id).qp, &mut wr, &mut bad))
}

#[inline]
pub unsafe fn rdma_post_writev(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    sgl: *mut ibv_sge,
    nsge: c_int,
    flags: c_int,
    remote_addr: u64,
    rkey: u32,
) -> c_int {
    let mut wr = std::mem::zeroed::<ibv_send_wr>();
    wr.wr_id = context as u64;
    wr.next = ptr::null::<ibv_send_wr>() as *mut _;
    wr.sg_list = sgl;
    wr.num_sge = nsge;
    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
    wr.send_flags = flags as c_uint;
    wr.wr = wr_t {
        rdma: rdma_t { remote_addr, rkey },
    };
    let mut bad = ptr::null::<ibv_send_wr>() as *mut _;

    rdma_seterrno(ibv_post_send((*id).qp, &mut wr, &mut bad))
}

#[inline]
pub unsafe fn rdma_post_recv(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    addr: *mut c_void,
    length: usize,
    mr: *mut ibv_mr,
) -> c_int {
    assert!(
        addr >= (*mr).addr && (addr as usize + length <= (*mr).addr as usize + (*mr).length),
        "invalid addr={} and length={}",
        addr as usize,
        length,
    );
    let mut sge = ibv_sge {
        addr: addr as u64,
        length: length as u32,
        lkey: (*mr).lkey,
    };
    let nsge = 1;
    rdma_post_recvv(id, context, &mut sge, nsge)
}

#[inline]
pub unsafe fn rdma_post_send(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    addr: *mut c_void,
    length: usize,
    mr: *mut ibv_mr,
    flags: c_int,
) -> c_int {
    let mut sge = ibv_sge {
        addr: addr as u64,
        length: length as u32,
        lkey: if !mr.is_null() { (*mr).lkey } else { 0 },
    };
    let nsge = 1;
    rdma_post_sendv(id, context, &mut sge, nsge, flags)
}

#[inline]
pub unsafe fn rdma_post_read(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    addr: *mut c_void,
    length: usize,
    mr: *mut ibv_mr,
    flags: c_int,
    remote_addr: u64,
    rkey: u32,
) -> c_int {
    let mut sge = ibv_sge {
        addr: addr as u64,
        length: length as u32,
        lkey: (*mr).lkey,
    };
    let nsge = 1;
    rdma_post_readv(id, context, &mut sge, nsge, flags, remote_addr, rkey)
}

#[inline]
pub unsafe fn rdma_post_write(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    addr: *mut c_void,
    length: usize,
    mr: *mut ibv_mr,
    flags: c_int,
    remote_addr: u64,
    rkey: u32,
) -> c_int {
    let mut sge = ibv_sge {
        addr: addr as u64,
        length: length as u32,
        lkey: if !mr.is_null() { (*mr).lkey } else { 0 },
    };
    let nsge = 1;
    rdma_post_writev(id, context, &mut sge, nsge, flags, remote_addr, rkey)
}

#[inline]
pub unsafe fn rdma_post_ud_send(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    addr: *mut c_void,
    length: usize,
    mr: *mut ibv_mr,
    flags: c_int,
    ah: *mut ibv_ah,
    remote_qpn: u32,
) -> c_int {
    let mut sge = ibv_sge {
        addr: addr as u64,
        length: length as u32,
        lkey: if !mr.is_null() { (*mr).lkey } else { 0 },
    };

    let mut wr = std::mem::zeroed::<ibv_send_wr>();
    wr.wr_id = context as u64;
    wr.next = ptr::null::<ibv_send_wr>() as *mut _;
    wr.sg_list = &mut sge;
    wr.num_sge = 1;
    wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
    wr.send_flags = flags as c_uint;
    wr.wr = wr_t {
        ud: ud_t {
            ah,
            remote_qpn,
            remote_qkey: RDMA_UDP_QKEY,
        },
    };
    let mut bad = ptr::null::<ibv_send_wr>() as *mut _;

    rdma_seterrno(ibv_post_send((*id).qp, &mut wr, &mut bad))
}

#[inline]
pub unsafe fn rdma_get_send_comp(id: *mut rdma_cm_id, wc: *mut ibv_wc) -> c_int {
    let mut ret: c_int;
    let mut cq = ptr::null::<ibv_cq>() as *mut _;
    let mut context = ptr::null::<c_void>() as *mut _;
    let nevents = 1;
    let num_entries = 1;
    let solicited_only = 0;

    loop {
        ret = ibv_poll_cq((*id).send_cq, num_entries, wc);
        if ret != 0 {
            break;
        }
        ret = ibv_req_notify_cq((*id).send_cq, solicited_only);
        if ret != 0 {
            return rdma_seterrno(ret);
        }
        ret = ibv_poll_cq((*id).send_cq, num_entries, wc);
        if ret != 0 {
            break;
        }
        ret = ibv_get_cq_event((*id).send_cq_channel, &mut cq, &mut context);
        if ret != 0 {
            return ret;
        }

        assert!(cq == (*id).send_cq && context as *mut rdma_cm_id == id);
        ibv_ack_cq_events((*id).send_cq, nevents);
    }
    if ret < 0 {
        rdma_seterrno(ret)
    } else {
        ret
    }
}

#[inline]
pub unsafe fn rdma_get_recv_comp(id: *mut rdma_cm_id, wc: *mut ibv_wc) -> c_int {
    let mut ret: c_int;
    let mut cq = ptr::null::<ibv_cq>() as *mut _;
    let mut context = ptr::null::<c_void>() as *mut _;
    let nevents = 1;
    let num_entries = 1;
    let solicited_only = 0;
    loop {
        ret = ibv_poll_cq((*id).recv_cq, num_entries, wc);
        if ret != 0 {
            break;
        }
        ret = ibv_req_notify_cq((*id).recv_cq, solicited_only);
        if ret != 0 {
            return rdma_seterrno(ret);
        }
        ret = ibv_poll_cq((*id).recv_cq, num_entries, wc);
        if ret != 0 {
            break;
        }
        ret = ibv_get_cq_event((*id).recv_cq_channel, &mut cq, &mut context);
        if ret != 0 {
            return ret;
        }

        assert!(cq == (*id).recv_cq && context as *mut rdma_cm_id == id);
        ibv_ack_cq_events((*id).recv_cq, nevents);
    }
    if ret < 0 {
        rdma_seterrno(ret)
    } else {
        ret
    }
}
