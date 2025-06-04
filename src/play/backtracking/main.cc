#include <iostream>
#include <stdexcept>

#include <boost/stacktrace.hpp>

void foo() {
    throw std::runtime_error("An error occurred in foo");
    std::cout << "foo" << std::endl;
}

void bar() {
    foo();
    std::cout << "bar" << std::endl;
}

int main(int argc, char* argv[]) {
    try {
        bar();
    } catch (const std::exception& e) {
        std::cout << "Caught exception: " << e.what() << std::endl;
        std::cout << "Stacktrace:\n"
                  << boost::stacktrace::stacktrace() << std::endl;
    }
    std::cout << "backtracking play" << std::endl;
    return 0;
}
