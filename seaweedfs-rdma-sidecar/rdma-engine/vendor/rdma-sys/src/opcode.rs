pub mod ibv_opcode {
    use paste::paste;
    pub type Type = std::os::raw::c_int;
    macro_rules! concat_ibv_opcode {
        ($transport: expr, $op : expr ) => {
            paste! {
                pub const [<IBV_OPCODE_ $transport _ $op>]: Type = [<IBV_OPCODE_ $transport>] + [<IBV_OPCODE_ $op>] ;
            }
        };
    }

    /* transport types -- just used to define real constants */
    pub const IBV_OPCODE_RC: Type = 0x00;
    pub const IBV_OPCODE_UC: Type = 0x20;
    pub const IBV_OPCODE_RD: Type = 0x40;
    pub const IBV_OPCODE_UD: Type = 0x60;
    /* operations -- just used to define real constants */
    pub const IBV_OPCODE_SEND_FIRST: Type = 0x00;
    pub const IBV_OPCODE_SEND_MIDDLE: Type = 0x01;
    pub const IBV_OPCODE_SEND_LAST: Type = 0x02;
    pub const IBV_OPCODE_SEND_LAST_WITH_IMMEDIATE: Type = 0x03;
    pub const IBV_OPCODE_SEND_ONLY: Type = 0x04;
    pub const IBV_OPCODE_SEND_ONLY_WITH_IMMEDIATE: Type = 0x05;
    pub const IBV_OPCODE_RDMA_WRITE_FIRST: Type = 0x06;
    pub const IBV_OPCODE_RDMA_WRITE_MIDDLE: Type = 0x07;
    pub const IBV_OPCODE_RDMA_WRITE_LAST: Type = 0x08;
    pub const IBV_OPCODE_RDMA_WRITE_LAST_WITH_IMMEDIATE: Type = 0x09;
    pub const IBV_OPCODE_RDMA_WRITE_ONLY: Type = 0x0a;
    pub const IBV_OPCODE_RDMA_WRITE_ONLY_WITH_IMMEDIATE: Type = 0x0b;
    pub const IBV_OPCODE_RDMA_READ_REQUEST: Type = 0x0c;
    pub const IBV_OPCODE_RDMA_READ_RESPONSE_FIRST: Type = 0x0d;
    pub const IBV_OPCODE_RDMA_READ_RESPONSE_MIDDLE: Type = 0x0e;
    pub const IBV_OPCODE_RDMA_READ_RESPONSE_LAST: Type = 0x0f;
    pub const IBV_OPCODE_RDMA_READ_RESPONSE_ONLY: Type = 0x10;
    pub const IBV_OPCODE_ACKNOWLEDGE: Type = 0x11;
    pub const IBV_OPCODE_ATOMIC_ACKNOWLEDGE: Type = 0x12;
    pub const IBV_OPCODE_COMPARE_SWAP: Type = 0x13;
    pub const IBV_OPCODE_FETCH_ADD: Type = 0x14;

    /* RC */
    concat_ibv_opcode!(RC, SEND_FIRST);
    concat_ibv_opcode!(RC, SEND_MIDDLE);
    concat_ibv_opcode!(RC, SEND_LAST);
    concat_ibv_opcode!(RC, SEND_LAST_WITH_IMMEDIATE);
    concat_ibv_opcode!(RC, SEND_ONLY);
    concat_ibv_opcode!(RC, SEND_ONLY_WITH_IMMEDIATE);
    concat_ibv_opcode!(RC, RDMA_WRITE_FIRST);
    concat_ibv_opcode!(RC, RDMA_WRITE_MIDDLE);
    concat_ibv_opcode!(RC, RDMA_WRITE_LAST);
    concat_ibv_opcode!(RC, RDMA_WRITE_LAST_WITH_IMMEDIATE);
    concat_ibv_opcode!(RC, RDMA_WRITE_ONLY);
    concat_ibv_opcode!(RC, RDMA_WRITE_ONLY_WITH_IMMEDIATE);
    concat_ibv_opcode!(RC, RDMA_READ_REQUEST);
    concat_ibv_opcode!(RC, RDMA_READ_RESPONSE_FIRST);
    concat_ibv_opcode!(RC, RDMA_READ_RESPONSE_MIDDLE);
    concat_ibv_opcode!(RC, RDMA_READ_RESPONSE_LAST);
    concat_ibv_opcode!(RC, RDMA_READ_RESPONSE_ONLY);
    concat_ibv_opcode!(RC, ACKNOWLEDGE);
    concat_ibv_opcode!(RC, ATOMIC_ACKNOWLEDGE);
    concat_ibv_opcode!(RC, COMPARE_SWAP);
    concat_ibv_opcode!(RC, FETCH_ADD);

    /* UC */
    concat_ibv_opcode!(UC, SEND_FIRST);
    concat_ibv_opcode!(UC, SEND_MIDDLE);
    concat_ibv_opcode!(UC, SEND_LAST);
    concat_ibv_opcode!(UC, SEND_LAST_WITH_IMMEDIATE);
    concat_ibv_opcode!(UC, SEND_ONLY);
    concat_ibv_opcode!(UC, SEND_ONLY_WITH_IMMEDIATE);
    concat_ibv_opcode!(UC, RDMA_WRITE_FIRST);
    concat_ibv_opcode!(UC, RDMA_WRITE_MIDDLE);
    concat_ibv_opcode!(UC, RDMA_WRITE_LAST);
    concat_ibv_opcode!(UC, RDMA_WRITE_LAST_WITH_IMMEDIATE);
    concat_ibv_opcode!(UC, RDMA_WRITE_ONLY);
    concat_ibv_opcode!(UC, RDMA_WRITE_ONLY_WITH_IMMEDIATE);

    /* RD */
    concat_ibv_opcode!(RD, SEND_FIRST);
    concat_ibv_opcode!(RD, SEND_MIDDLE);
    concat_ibv_opcode!(RD, SEND_LAST);
    concat_ibv_opcode!(RD, SEND_LAST_WITH_IMMEDIATE);
    concat_ibv_opcode!(RD, SEND_ONLY);
    concat_ibv_opcode!(RD, SEND_ONLY_WITH_IMMEDIATE);
    concat_ibv_opcode!(RD, RDMA_WRITE_FIRST);
    concat_ibv_opcode!(RD, RDMA_WRITE_MIDDLE);
    concat_ibv_opcode!(RD, RDMA_WRITE_LAST);
    concat_ibv_opcode!(RD, RDMA_WRITE_LAST_WITH_IMMEDIATE);
    concat_ibv_opcode!(RD, RDMA_WRITE_ONLY);
    concat_ibv_opcode!(RD, RDMA_WRITE_ONLY_WITH_IMMEDIATE);
    concat_ibv_opcode!(RD, RDMA_READ_REQUEST);
    concat_ibv_opcode!(RD, RDMA_READ_RESPONSE_FIRST);
    concat_ibv_opcode!(RD, RDMA_READ_RESPONSE_MIDDLE);
    concat_ibv_opcode!(RD, RDMA_READ_RESPONSE_LAST);
    concat_ibv_opcode!(RD, RDMA_READ_RESPONSE_ONLY);
    concat_ibv_opcode!(RD, ACKNOWLEDGE);
    concat_ibv_opcode!(RD, ATOMIC_ACKNOWLEDGE);
    concat_ibv_opcode!(RD, COMPARE_SWAP);
    concat_ibv_opcode!(RD, FETCH_ADD);

    /* UD */
    concat_ibv_opcode!(UD, SEND_ONLY);
    concat_ibv_opcode!(UD, SEND_ONLY_WITH_IMMEDIATE);
}
