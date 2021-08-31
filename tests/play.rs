struct A {
    a: i32,
}

impl A {
    fn talk(&self) {
        println!("A.talk {}", self.a);
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

#[test]
fn inheritance() {
    let a1 = A { a: 1 };
    a1.talk();
    let b1 = B { a: a1, b: 2 };
    b1.walk();
    b1.a.talk();
    b1.talk();
}
