use std::marker::PhantomData;

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
struct A {
    a: i32,
}

impl A {
    fn talk(&self) {
        println!("A.talk {}", self.a);
    }
}

impl CanFly for A {
    fn fly(self: &mut A) {
        println!("A.fly {}", self.a);
    }
}

struct B {
    a: A,
    b: i32,
}

impl B {
    fn walk(&self) {
        println!("B.walk {}", self.b);
    }
}

impl std::ops::Deref for B {
    type Target = A;
    fn deref(&self) -> &Self::Target {
        &self.a
    }
}

impl std::ops::DerefMut for B {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.a
    }
}

trait CanFly {
    fn fly(&mut self);
}

fn talk(v: &mut dyn CanFly) {
    v.fly();
}

#[test]
#[ignore]
fn inheritance() {
    let mut a1 = A { a: 1 };
    a1.talk();
    let mut b1 = B { a: a1, b: 2 };
    b1.walk();
    b1.a.talk();
    b1.talk();

    talk(&mut a1);
    talk(&mut *b1);
}

#[test]
#[ignore]
fn ptr_equality() {
    let a1 = A { a: 1 };
    let a2 = A { a: 1 };
    assert_eq!(a1, a2);

    let ptr_1 = &a1;
    let ptr_2 = &a2;
    assert_eq!(ptr_1, ptr_2);
}

// `Option` has the kind `Type -> Type`,
// we'll represent it with `OptionFamily`
struct OptionFamily;
// `Result` has the kind `Type -> Type -> Type`,
// so we fill in one of the types with a concrete one
struct ResultFamily<E>(PhantomData<E>);

// I'll leave the implementation of `VecFamily` to you
struct VecFamily;

// This trait represents the `kind` `Type -> Type`
pub trait OneTypeParam<A> {
    // This represents the output of the function `Type -> Type`
    // for a specific argument `A`.
    type This;
}

impl<A> OneTypeParam<A> for OptionFamily {
    // `OptionFamily` represents `Type -> Type`,
    // so filling in the first argument means
    // `Option<A>`
    type This = Option<A>;
}

impl<A, E> OneTypeParam<A> for ResultFamily<E> {
    // note how all results in this family have `E` as the error type
    // This is similar to how currying works in functional languages
    type This = Result<A, E>;
}

impl<A> OneTypeParam<A> for VecFamily {
    type This = Vec<A>;
}

// Option<A> == This<OptionFamily, A>
pub type This<T, A> = <T as OneTypeParam<A>>::This;

trait Functor<A, B>: OneTypeParam<A> + OneTypeParam<B> {
    fn map<F>(self, this: This<Self, A>, f: F) -> This<Self, B>
    where
        F: Fn(A) -> B + Copy;
}

impl<A, B> Functor<A, B> for OptionFamily {
    fn map<F>(self, this: This<Self, A>, f: F) -> This<Self, B>
    where
        F: Fn(A) -> B + Copy,
    {
        // I'm not cheating!
        this.map(f)
    }
}

// try out `VecFamily`, it doesn't need to be optimal, it just needs to work!
impl<A, B> Functor<A, B> for VecFamily {
    fn map<F>(self, this: This<Self, A>, f: F) -> This<Self, B>
    where
        F: Fn(A) -> B + Copy,
    {
        this.into_iter().map(f).collect()
    }
}

trait Monad<A, B>: Functor<A, B> {
    fn bind<F>(self, a: This<Self, A>, f: F) -> This<Self, B>
    where
        F: Fn(A) -> This<Self, B> + Copy;
}

impl<A, B> Monad<A, B> for OptionFamily {
    fn bind<F>(self, this: This<Self, A>, f: F) -> This<Self, B>
    where
        F: Fn(A) -> This<Self, B> + Copy,
    {
        // It fits 😉
        this.and_then(f)
    }
}

// try out `VecFamily`, it doesn't need to be optimal, it just needs to work!
impl<A, B> Monad<A, B> for VecFamily {
    fn bind<F>(self, this: This<Self, A>, f: F) -> This<Self, B>
    where
        F: Fn(A) -> This<Self, B> + Copy,
    {
        this.into_iter().flat_map(f).collect()
    }
}

// #![allow(unused)]
fn main() {
    struct Inspector<T>(T, &'static str, Box<for<'r> fn(&'r T) -> String>);

    impl<T> Drop for Inspector<T> {
        fn drop(&mut self) {
            // The `self.2` call could access a borrow e.g. if `T` is `&'a _`.
            println!(
                "Inspector({}, {}) unwittingly inspects expired data.",
                (self.2)(&self.0),
                self.1
            );
        }
    }
}

#[test]
fn lock() {
    use std::{
        sync::{Arc, Mutex},
        thread,
    };
    let mutex = Arc::new(Mutex::new(0));

    mutex.try_lock();

    use std::sync::RwLock;

    let lock = RwLock::new(1);

    lock.try_read()
}
