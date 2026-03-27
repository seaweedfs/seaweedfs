/// Constructs vector of shards.
///
/// # Example
/// ```rust
/// # #[macro_use] extern crate reed_solomon_erasure;
/// # use reed_solomon_erasure::*;
/// # fn main () {
/// let shards: Vec<Vec<u8>> = shards!([1, 2, 3],
///                                    [4, 5, 6]);
/// # }
/// ```
#[macro_export]
macro_rules! shards {
    (
        $( [ $( $x:expr ),* ] ),*
    ) => {{
        vec![ $( vec![ $( $x ),* ] ),* ]
    }}
}

/// Makes it easier to work with 2D slices, arrays, etc.
///
/// # Examples
/// ## Byte arrays on stack to `Vec<&[u8]>`
/// ```rust
/// # #[macro_use] extern crate reed_solomon_erasure;
/// # fn main () {
/// let array: [[u8; 3]; 2] = [[1, 2, 3],
///                             [4, 5, 6]];
///
/// let refs: Vec<&[u8]> =
///     convert_2D_slices!(array =>to_vec &[u8]);
/// # }
/// ```
/// ## Byte arrays on stack to `Vec<&mut [u8]>` (borrow mutably)
/// ```rust
/// # #[macro_use] extern crate reed_solomon_erasure;
/// # fn main () {
/// let mut array: [[u8; 3]; 2] = [[1, 2, 3],
///                                [4, 5, 6]];
///
/// let refs: Vec<&mut [u8]> =
///     convert_2D_slices!(array =>to_mut_vec &mut [u8]);
/// # }
/// ```
/// ## Byte arrays on stack to `SmallVec<[&mut [u8]; 32]>` (borrow mutably)
/// ```rust
/// # #[macro_use] extern crate reed_solomon_erasure;
/// # extern crate smallvec;
/// # use smallvec::SmallVec;
/// # fn main () {
/// let mut array: [[u8; 3]; 2] = [[1, 2, 3],
///                                [4, 5, 6]];
///
/// let refs: SmallVec<[&mut [u8]; 32]> =
///     convert_2D_slices!(array =>to_mut SmallVec<[&mut [u8]; 32]>,
///                        SmallVec::with_capacity);
/// # }
/// ```
/// ## Shard array to `SmallVec<[&mut [u8]; 32]>` (borrow mutably)
/// ```rust
/// # #[macro_use] extern crate reed_solomon_erasure;
/// # extern crate smallvec;
/// # use smallvec::SmallVec;
/// # fn main () {
/// let mut shards = shards!([1, 2, 3],
///                          [4, 5, 6]);
///
/// let refs: SmallVec<[&mut [u8]; 32]> =
///     convert_2D_slices!(shards =>to_mut SmallVec<[&mut [u8]; 32]>,
///                        SmallVec::with_capacity);
/// # }
/// ```
/// ## Shard array to `Vec<&mut [u8]>` (borrow mutably) into `SmallVec<[&mut [u8]; 32]>` (move)
/// ```rust
/// # #[macro_use] extern crate reed_solomon_erasure;
/// # extern crate smallvec;
/// # use smallvec::SmallVec;
/// # fn main () {
/// let mut shards = shards!([1, 2, 3],
///                          [4, 5, 6]);
///
/// let refs1 = convert_2D_slices!(shards =>to_mut_vec &mut [u8]);
///
/// let refs2: SmallVec<[&mut [u8]; 32]> =
///     convert_2D_slices!(refs1 =>into SmallVec<[&mut [u8]; 32]>,
///                        SmallVec::with_capacity);
/// # }
/// ```
#[macro_export]
macro_rules! convert_2D_slices {
    (
        $slice:expr =>into_vec $dst_type:ty
    ) => {
        convert_2D_slices!($slice =>into Vec<$dst_type>,
                           Vec::with_capacity)
    };
    (
        $slice:expr =>to_vec $dst_type:ty
    ) => {
        convert_2D_slices!($slice =>to Vec<$dst_type>,
                           Vec::with_capacity)
    };
    (
        $slice:expr =>to_mut_vec $dst_type:ty
    ) => {
        convert_2D_slices!($slice =>to_mut Vec<$dst_type>,
                           Vec::with_capacity)
    };
    (
        $slice:expr =>into $dst_type:ty, $with_capacity:path
    ) => {{
        let mut result: $dst_type =
            $with_capacity($slice.len());
        for i in $slice.into_iter() {
            result.push(i);
        }
        result
    }};
    (
        $slice:expr =>to $dst_type:ty, $with_capacity:path
    ) => {{
        let mut result: $dst_type =
            $with_capacity($slice.len());
        for i in $slice.iter() {
            result.push(i);
        }
        result
    }};
    (
        $slice:expr =>to_mut $dst_type:ty, $with_capacity:path
    ) => {{
        let mut result: $dst_type =
            $with_capacity($slice.len());
        for i in $slice.iter_mut() {
            result.push(i);
        }
        result
    }}
}

macro_rules! check_slices {
    (
        multi => $slices:expr
    ) => {{
        let size = $slices[0].as_ref().len();
        if size == 0 {
            return Err(Error::EmptyShard);
        }
        for slice in $slices.iter() {
            if slice.as_ref().len() != size {
                return Err(Error::IncorrectShardSize);
            }
        }
    }};
    (
        single => $slice_left:expr, single => $slice_right:expr
    ) => {{
        if $slice_left.as_ref().len() != $slice_right.as_ref().len() {
            return Err(Error::IncorrectShardSize);
        }
    }};
    (
        multi => $slices:expr, single => $single:expr
    ) => {{
        check_slices!(multi => $slices);

        check_slices!(single => $slices[0], single => $single);
    }};
    (
        multi => $slices_left:expr, multi => $slices_right:expr
    ) => {{
        check_slices!(multi => $slices_left);
        check_slices!(multi => $slices_right);

        check_slices!(single => $slices_left[0], single => $slices_right[0]);
    }}
}

macro_rules! check_slice_index {
    (
        all => $codec:expr, $index:expr
    ) => {{
        if $index >= $codec.total_shard_count {
            return Err(Error::InvalidIndex);
        }
    }};
    (
        data => $codec:expr, $index:expr
    ) => {{
        if $index >= $codec.data_shard_count {
            return Err(Error::InvalidIndex);
        }
    }};
    (
        parity => $codec:expr, $index:expr
    ) => {{
        if $index >= $codec.parity_shard_count {
            return Err(Error::InvalidIndex);
        }
    }};
}

macro_rules! check_piece_count {
    (
        all => $codec:expr, $pieces:expr
    ) => {{
        if $pieces.as_ref().len() < $codec.total_shard_count {
            return Err(Error::TooFewShards);
        }
        if $pieces.as_ref().len() > $codec.total_shard_count {
            return Err(Error::TooManyShards);
        }
    }};
    (
        data => $codec:expr, $pieces:expr
    ) => {{
        if $pieces.as_ref().len() < $codec.data_shard_count {
            return Err(Error::TooFewDataShards);
        }
        if $pieces.as_ref().len() > $codec.data_shard_count {
            return Err(Error::TooManyDataShards);
        }
    }};
    (
        parity => $codec:expr, $pieces:expr
    ) => {{
        if $pieces.as_ref().len() < $codec.parity_shard_count {
            return Err(Error::TooFewParityShards);
        }
        if $pieces.as_ref().len() > $codec.parity_shard_count {
            return Err(Error::TooManyParityShards);
        }
    }};
    (
        parity_buf => $codec:expr, $pieces:expr
    ) => {{
        if $pieces.as_ref().len() < $codec.parity_shard_count {
            return Err(Error::TooFewBufferShards);
        }
        if $pieces.as_ref().len() > $codec.parity_shard_count {
            return Err(Error::TooManyBufferShards);
        }
    }};
}
