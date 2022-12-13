use std::ops::{Add, Div, Sub};

pub trait Number:
    Add<Output = Self>
    + Sub<Output = Self>
    + Div<Output = Self>
    + PartialEq
    + Copy
{
    fn one() -> Self;
}

impl Number for u32 {
    fn one() -> Self {
        1
    }
}

impl Number for usize {
    fn one() -> Self {
        1
    }
}

pub fn ceil_dev<T: Number>(a: T, b: T) -> T {
    (a + b - T::one()) / b
}

pub fn floor_dev<T: Number>(a: T, b: T) -> T {
    a / b
}
