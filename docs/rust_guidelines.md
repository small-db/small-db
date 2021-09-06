# Rust Guidelines

## Naming Conventions

For a given struct like `page` with type `Page`, the following rules apply:

- `page` is the struct or a reference to the struct.

- `page_rc` is a reference to the struct, with the `Rc` wrapper. With type `<Rc<RefCell<Page>>>`.

## Borrow & BorrowMut Safety
