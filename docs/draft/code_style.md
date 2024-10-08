## Code Style

### Lock Range

> Here I agree with someone who wrote "If you need recursive locks, your code is too complex." After experiencing several deadlocks stemming from ridiculously complex code, I can say that all operations within a critical section should only be memory operations - assignment, memcpy etc - no syscalls, no locks and no calls of complex functions.
>
> [Is there a crate that implements a reentrant rwlock? : rust](https://www.reddit.com/r/rust/comments/a2jht3/comment/eb3dhak/?utm_source=share&utm_medium=web2x&context=3)

### API Path

API from standard and third-party crates should be prefixed with the lowest module name, while internal APIs should be used without any prefix. This allows programmers to quickly identify whether an API is implemented by SmallDB itself.



