pub type FloInstanceId = u64;

pub const NULL_INSTANCE_ID: FloInstanceId = 0;

pub fn generate_new() -> FloInstanceId {
    let mut value = 0;
    while value == 0 {
        value = ::rand::random();
    }
    value
}

named!{pub parse_flo_instance_id<FloInstanceId>, map_res!(::nom::be_u64, |val| {
    if val == 0 {
        Err(())
    } else {
        Ok(val)
    }
})}

named!{pub parse_optional_flo_instance_id<Option<FloInstanceId>>, map!(::nom::be_u64, |val| {
    if val > 0 {
        Some(val)
    } else {
        None
    }
} )}
