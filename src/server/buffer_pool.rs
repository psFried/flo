use std::cmp::max;

const MINIMUM_BUFFER_LENGTH: usize = 8 * 1024;

pub struct BufferPool {
    buffers: Vec<Vec<u8>>
}

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {
            buffers: Vec::with_capacity(16)
        }
    }

    pub fn get_buffer(&mut self, min_size: usize) -> Vec<u8> {
        let buffer_index = self.buffers.iter().position(|buffer| {
            buffer.len() >= min_size
        });

        buffer_index.map(|index| {
            self.buffers.remove(index)
        }).unwrap_or_else(|| {
            let buffer_length = max(MINIMUM_BUFFER_LENGTH, min_size.next_power_of_two());
            vec![0; buffer_length]
        })
    }

    pub fn return_buffer(&mut self, buffer: Vec<u8>) {
        self.buffers.push(buffer);
    }
}


#[test]
fn get_buffer_returns_buffer_with_capacity_rounded_up_to_the_next_power_of_2() {
    let mut subject = BufferPool::new();

    let result = subject.get_buffer((1024 * 4) + 1); //4kb + 1 byte
    assert_eq!(8 * 1024, result.len());
    assert!(result.iter().all(|b| *b == 0u8));
}


