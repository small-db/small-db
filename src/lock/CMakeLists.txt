add_library(small_lock
    lock_manager.cc
    lock_manager.h
)

add_library(small::lock ALIAS small_lock)
