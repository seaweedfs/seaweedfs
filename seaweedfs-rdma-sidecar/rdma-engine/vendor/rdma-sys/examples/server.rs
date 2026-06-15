//! This demo shows how to establish a connection between server and client
//! and send msg to the other end.
//!
//! You can try this example by running:
//!
//!     cargo run --example server
//!
//! And then start client in another terminal by running:
//!
//!     cargo run --example client <server_ip> <port>
//!
//! The default port is 7471.

use rdma_sys::*;
use std::ptr::null_mut;

static SERVER: &str = "0.0.0.0\0";
static PORT: &str = "7471\0";

fn run() -> i32 {
    let mut send_msg = vec![1_u8; 16];
    let mut recv_msg = vec![0_u8; 16];
    let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    let mut res: *mut rdma_addrinfo = null_mut();
    hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
    hints.ai_port_space = rdma_port_space::RDMA_PS_TCP.try_into().unwrap();
    let mut ret = unsafe {
        rdma_getaddrinfo(
            SERVER.as_ptr().cast(),
            PORT.as_ptr().cast(),
            &hints,
            &mut res,
        )
    };

    if ret != 0 {
        println!("rdma_getaddrinfo");
        return ret;
    }

    let mut listen_id = null_mut();
    let mut id = null_mut();

    let mut init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    init_attr.cap.max_send_wr = 1;
    init_attr.cap.max_recv_wr = 1;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_inline_data = 16;
    init_attr.sq_sig_all = 1;
    ret = unsafe { rdma_create_ep(&mut listen_id, res, null_mut(), &mut init_attr) };
    // Check to see if we got inline data allowed or not
    if ret != 0 {
        println!("rdma_create_ep");
        unsafe {
            rdma_freeaddrinfo(res);
        }
        return ret;
    }
    ret = unsafe { rdma_listen(listen_id, 0) };
    if ret != 0 {
        println!("rdma_listen");
        unsafe {
            rdma_destroy_ep(listen_id);
        }
        return ret;
    }

    ret = unsafe { rdma_get_request(listen_id, &mut id) };
    if ret != 0 {
        println!("rdma_get_request");
        unsafe {
            rdma_destroy_ep(listen_id);
        }
        return ret;
    }

    let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    ret = unsafe {
        ibv_query_qp(
            (*id).qp,
            &mut qp_attr,
            ibv_qp_attr_mask::IBV_QP_CAP.0.try_into().unwrap(),
            &mut init_attr,
        )
    };

    if ret != 0 {
        println!("ibv_query_qp");
        unsafe {
            rdma_destroy_ep(id);
        }
        return ret;
    }

    let mut send_flags = 0_u32;
    if init_attr.cap.max_inline_data >= 16 {
        send_flags = ibv_send_flags::IBV_SEND_INLINE.0;
    } else {
        println!("rdma_server: device doesn't support IBV_SEND_INLINE, using sge sends");
    }

    let recv_mr = unsafe { rdma_reg_msgs(id, recv_msg.as_mut_ptr().cast(), 16) };
    if recv_mr.is_null() {
        ret = -1;
        println!("rdma_reg_msgs for recv_msg");
        unsafe {
            rdma_dereg_mr(recv_mr);
        }
        return ret;
    }

    let mut send_mr = null_mut();
    if (send_flags & ibv_send_flags::IBV_SEND_INLINE.0) == 0 {
        send_mr = unsafe { rdma_reg_msgs(id, send_msg.as_mut_ptr().cast(), 16) };
        if send_mr.is_null() {
            ret = -1;
            println!("rdma_reg_msgs for send_msg");
            unsafe {
                rdma_dereg_mr(recv_mr);
            }
            return ret;
        }
    }
    ret = unsafe { rdma_post_recv(id, null_mut(), recv_msg.as_mut_ptr().cast(), 16, recv_mr) };

    if ret != 0 {
        println!("rdma_post_recv");
        unsafe {
            rdma_dereg_mr(recv_mr);
        }
        return ret;
    }

    ret = unsafe { rdma_accept(id, null_mut()) };
    if ret != 0 {
        println!("rdma_accept");
        if (send_flags & ibv_send_flags::IBV_SEND_INLINE.0) == 0 {
            unsafe { rdma_dereg_mr(send_mr) };
        }
        return ret;
    }

    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    while ret == 0 {
        ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_recv_comp");
        unsafe {
            rdma_disconnect(id);
        }
        return ret;
    }
    println!("rdma_server: recv msg : {:?}", recv_msg);
    ret = unsafe {
        rdma_post_send(
            id,
            null_mut(),
            send_msg.as_mut_ptr().cast(),
            16,
            send_mr,
            send_flags.try_into().unwrap(),
        )
    };
    if ret != 0 {
        println!("rdma_post_send");
        unsafe {
            rdma_disconnect(id);
        }
        return ret;
    }

    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_send_comp");
    } else {
        ret = 0;
    }
    ret
}

fn main() {
    println!("rdma_server: start");
    let ret = run();
    if ret != 0 {
        println!(
            "rdma_server: ret error {:?}",
            std::io::Error::from_raw_os_error(-ret)
        );
        if ret == -1 {
            println!(
                "rdma_server: last os error {:?}",
                std::io::Error::last_os_error()
            );
        }
    }
    println!("rdma_server: end");
}
