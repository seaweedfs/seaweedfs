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
use std::{env, process::exit, ptr::null_mut};

fn run(ip: &str, port: &str) -> i32 {
    let mut send_msg = vec![1_u8; 16];
    let mut recv_msg = vec![0_u8; 16];
    let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    let mut res: *mut rdma_addrinfo = null_mut();

    hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as i32;
    let mut ret =
        unsafe { rdma_getaddrinfo(ip.as_ptr().cast(), port.as_ptr().cast(), &hints, &mut res) };

    if ret != 0 {
        println!("rdma_getaddrinfo");
        return ret;
    }

    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    let mut id: *mut rdma_cm_id = null_mut();
    attr.cap.max_send_wr = 1;
    attr.cap.max_recv_wr = 1;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 16;
    attr.qp_context = id.cast();
    attr.sq_sig_all = 1;
    ret = unsafe { rdma_create_ep(&mut id, res, null_mut(), &mut attr) };
    // Check to see if we got inline data allowed or not
    let mut send_flags = 0_u32;
    if attr.cap.max_inline_data >= 16 {
        send_flags = ibv_send_flags::IBV_SEND_INLINE.0;
    } else {
        println!("rdma_client: device doesn't support IBV_SEND_INLINE, using sge sends");
    }

    if ret != 0 {
        println!("rdma_create_ep");
        unsafe {
            rdma_freeaddrinfo(res);
        }
        return ret;
    }

    let mr = unsafe { rdma_reg_msgs(id, recv_msg.as_mut_ptr().cast(), 16) };
    if mr.is_null() {
        println!("rdma_reg_msgs for recv_msg");
        unsafe {
            rdma_destroy_ep(id);
        }
        return -1;
    }

    let mut send_mr = null_mut();
    if (send_flags & ibv_send_flags::IBV_SEND_INLINE.0) as u32 == 0 {
        println!("flags {:?}", send_flags);
        send_mr = unsafe { rdma_reg_msgs(id, send_msg.as_mut_ptr().cast(), 16) };
        if send_mr.is_null() {
            println!("rdma_reg_msgs for send_msg");
            unsafe {
                rdma_dereg_mr(mr);
            }
            return -1;
        }
    }

    ret = unsafe { rdma_post_recv(id, null_mut(), recv_msg.as_mut_ptr().cast(), 16, mr) };
    if ret != 0 {
        println!("rdma_post_recv");
        if (send_flags & ibv_send_flags::IBV_SEND_INLINE.0) as u32 == 0 {
            unsafe { rdma_dereg_mr(send_mr) };
        }
        return ret;
    }

    ret = unsafe { rdma_connect(id, null_mut()) };
    if ret != 0 {
        println!("rdma_connect");
        unsafe {
            rdma_disconnect(id);
        }
        return ret;
    }

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

    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_send_comp");
        unsafe {
            rdma_disconnect(id);
        }
        return ret;
    }

    ret = 0;
    while ret == 0 {
        ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
    }
    println!("rdma_client: recv msg : {:?}", recv_msg);
    if ret < 0 {
        println!("rdma_get_recv_comp");
    } else {
        ret = 0;
    }

    ret
}

fn main() {
    println!("rdma_client: start");
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("usage : cargo run --example client <server_ip> <port>");
        println!("input : {:?}", args);
        exit(-1);
    }
    let ip = args.get(1).unwrap().as_str();
    let port = args.get(2).unwrap().as_str();

    let ret = run(ip, port);

    if ret != 0 {
        println!(
            "rdma_client: ret error {:?}",
            std::io::Error::from_raw_os_error(-ret)
        );
        if ret == -1 {
            println!(
                "rdma_client: last os error {:?}",
                std::io::Error::last_os_error()
            );
        }
    }
    println!("rdma_client: end");
}
