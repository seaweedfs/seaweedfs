extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;

use super::{fill_random, option_shards_into_shards, shards_into_option_shards};
use crate::galois_16::ReedSolomon;

macro_rules! make_random_shards {
    ($per_shard:expr, $size:expr) => {{
        let mut shards = Vec::with_capacity(20);
        for _ in 0..$size {
            shards.push(vec![[0; 2]; $per_shard]);
        }

        for s in shards.iter_mut() {
            fill_random(s);
        }

        shards
    }};
}

#[test]
fn correct_field_order_restriction() {
    const ORDER: usize = 1 << 16;

    assert!(ReedSolomon::new(ORDER, 1).is_err());
    assert!(ReedSolomon::new(1, ORDER).is_err());

    // way too slow, because it needs to build a 65536*65536 vandermonde matrix
    // assert!(ReedSolomon::new(ORDER - 1, 1).is_ok());
    assert!(ReedSolomon::new(1, ORDER - 1).is_ok());
}

quickcheck! {
    fn qc_encode_verify_reconstruct_verify(data: usize,
                                           parity: usize,
                                           corrupt: usize,
                                           size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let corrupt = corrupt % (parity + 1);

        let mut corrupt_pos_s = Vec::with_capacity(corrupt);
        for _ in 0..corrupt {
            let mut pos = rand::random::<usize>() % (data + parity);

            while let Some(_) = corrupt_pos_s.iter().find(|&&x| x == pos) {
                pos = rand::random::<usize>() % (data + parity);
            }

            corrupt_pos_s.push(pos);
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        {
            let mut refs =
                convert_2D_slices!(expect =>to_mut_vec &mut [[u8; 2]]);

            r.encode(&mut refs).unwrap();
        }

        let expect = expect;

        let mut shards = expect.clone();

        // corrupt shards
        for &p in corrupt_pos_s.iter() {
            fill_random(&mut shards[p]);
        }
        let mut slice_present = vec![true; data + parity];
        for &p in corrupt_pos_s.iter() {
            slice_present[p] = false;
        }

        // reconstruct
        {
            let mut refs: Vec<_> = shards.iter_mut()
                .map(|i| &mut i[..])
                .zip(slice_present.iter().cloned())
                .collect();

            r.reconstruct(&mut refs[..]).unwrap();
        }

        ({
            let refs =
                convert_2D_slices!(expect =>to_vec &[[u8; 2]]);

            r.verify(&refs).unwrap()
        })
            &&
            expect == shards
            &&
            ({
                let refs =
                    convert_2D_slices!(shards =>to_vec &[[u8; 2]]);

                r.verify(&refs).unwrap()
            })
    }

    fn qc_encode_verify_reconstruct_verify_shards(data: usize,
                                                  parity: usize,
                                                  corrupt: usize,
                                                  size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let corrupt = corrupt % (parity + 1);

        let mut corrupt_pos_s = Vec::with_capacity(corrupt);
        for _ in 0..corrupt {
            let mut pos = rand::random::<usize>() % (data + parity);

            while let Some(_) = corrupt_pos_s.iter().find(|&&x| x == pos) {
                pos = rand::random::<usize>() % (data + parity);
            }

            corrupt_pos_s.push(pos);
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        r.encode(&mut expect).unwrap();

        let expect = expect;

        let mut shards = shards_into_option_shards(expect.clone());

        // corrupt shards
        for &p in corrupt_pos_s.iter() {
            shards[p] = None;
        }

        // reconstruct
        r.reconstruct(&mut shards).unwrap();

        let shards = option_shards_into_shards(shards);

        r.verify(&expect).unwrap()
            && expect == shards
            && r.verify(&shards).unwrap()
    }

    fn qc_verify(data: usize,
                 parity: usize,
                 corrupt: usize,
                 size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let corrupt = corrupt % (parity + 1);

        let mut corrupt_pos_s = Vec::with_capacity(corrupt);
        for _ in 0..corrupt {
            let mut pos = rand::random::<usize>() % (data + parity);

            while let Some(_) = corrupt_pos_s.iter().find(|&&x| x == pos) {
                pos = rand::random::<usize>() % (data + parity);
            }

            corrupt_pos_s.push(pos);
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        {
            let mut refs =
                convert_2D_slices!(expect =>to_mut_vec &mut [[u8; 2]]);

            r.encode(&mut refs).unwrap();
        }

        let expect = expect;

        let mut shards = expect.clone();

        // corrupt shards
        for &p in corrupt_pos_s.iter() {
            fill_random(&mut shards[p]);
        }

        ({
            let refs =
                convert_2D_slices!(expect =>to_vec &[[u8; 2]]);

            r.verify(&refs).unwrap()
        })
            &&
            ((corrupt > 0 && expect != shards)
             || (corrupt == 0 && expect == shards))
            &&
            ({
                let refs =
                    convert_2D_slices!(shards =>to_vec &[[u8; 2]]);

                (corrupt > 0 && !r.verify(&refs).unwrap())
                    || (corrupt == 0 && r.verify(&refs).unwrap())
            })
    }

    fn qc_verify_shards(data: usize,
                        parity: usize,
                        corrupt: usize,
                        size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let corrupt = corrupt % (parity + 1);

        let mut corrupt_pos_s = Vec::with_capacity(corrupt);
        for _ in 0..corrupt {
            let mut pos = rand::random::<usize>() % (data + parity);

            while let Some(_) = corrupt_pos_s.iter().find(|&&x| x == pos) {
                pos = rand::random::<usize>() % (data + parity);
            }

            corrupt_pos_s.push(pos);
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        r.encode(&mut expect).unwrap();

        let expect = expect;

        let mut shards = expect.clone();

        // corrupt shards
        for &p in corrupt_pos_s.iter() {
            fill_random(&mut shards[p]);
        }

        r.verify(&expect).unwrap()
            &&
            ((corrupt > 0 && expect != shards)
             || (corrupt == 0 && expect == shards))
            &&
            ((corrupt > 0 && !r.verify(&shards).unwrap())
             || (corrupt == 0 && r.verify(&shards).unwrap()))
    }

    fn qc_encode_sep_same_as_encode(data: usize,
                                    parity: usize,
                                    size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        let mut shards = expect.clone();

        {
            let mut refs =
                convert_2D_slices!(expect =>to_mut_vec &mut [[u8; 2]]);

            r.encode(&mut refs).unwrap();
        }

        let expect = expect;

        {
            let (data, parity) = shards.split_at_mut(data);

            let data_refs =
                convert_2D_slices!(data =>to_mut_vec &[[u8; 2]]);

            let mut parity_refs =
                convert_2D_slices!(parity =>to_mut_vec &mut [[u8; 2]]);

            r.encode_sep(&data_refs, &mut parity_refs).unwrap();
        }

        let shards = shards;

        expect == shards
    }

    fn qc_encode_sep_same_as_encode_shards(data: usize,
                                           parity: usize,
                                           size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        let mut shards = expect.clone();

        r.encode(&mut expect).unwrap();

        let expect = expect;

        {
            let (data, parity) = shards.split_at_mut(data);

            r.encode_sep(data, parity).unwrap();
        }

        let shards = shards;

        expect == shards
    }

    fn qc_encode_single_same_as_encode(data: usize,
                                       parity: usize,
                                       size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        let mut shards = expect.clone();

        {
            let mut refs =
                convert_2D_slices!(expect =>to_mut_vec &mut [[u8; 2]]);

            r.encode(&mut refs).unwrap();
        }

        let expect = expect;

        {
            let mut refs =
                convert_2D_slices!(shards =>to_mut_vec &mut [[u8; 2]]);

            for i in 0..data {
                r.encode_single(i, &mut refs).unwrap();
            }
        }

        let shards = shards;

        expect == shards
    }

    fn qc_encode_single_same_as_encode_shards(data: usize,
                                              parity: usize,
                                              size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        let mut shards = expect.clone();

        r.encode(&mut expect).unwrap();

        let expect = expect;

        for i in 0..data {
            r.encode_single(i, &mut shards).unwrap();
        }

        let shards = shards;

        expect == shards
    }

    fn qc_encode_single_sep_same_as_encode(data: usize,
                                           parity: usize,
                                           size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        let mut shards = expect.clone();

        {
            let mut refs =
                convert_2D_slices!(expect =>to_mut_vec &mut [[u8; 2]]);

            r.encode(&mut refs).unwrap();
        }

        let expect = expect;

        {
            let (data_shards, parity_shards) = shards.split_at_mut(data);

            let data_refs =
                convert_2D_slices!(data_shards =>to_mut_vec &[[u8; 2]]);

            let mut parity_refs =
                convert_2D_slices!(parity_shards =>to_mut_vec &mut [[u8; 2]]);

            for i in 0..data {
                r.encode_single_sep(i, data_refs[i], &mut parity_refs).unwrap();
            }
        }

        let shards = shards;

        expect == shards
    }

    fn qc_encode_single_sep_same_as_encode_shards(data: usize,
                                                  parity: usize,
                                                  size: usize) -> bool {
        let data = 1 + data % 255;
        let mut parity = 1 + parity % 255;
        if data + parity > 256 {
            parity -= data + parity - 256;
        }

        let size = 1 + size % 1_000_000;

        let r = ReedSolomon::new(data, parity).unwrap();

        let mut expect = make_random_shards!(size, data + parity);
        let mut shards = expect.clone();

        r.encode(&mut expect).unwrap();

        let expect = expect;

        {
            let (data_shards, parity_shards) = shards.split_at_mut(data);

            for i in 0..data {
                r.encode_single_sep(i, &data_shards[i], parity_shards).unwrap();
            }
        }

        let shards = shards;

        expect == shards
    }
}
