#include <backward.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <iomanip>

class TracedException : public std::runtime_error {
public:
  TracedException() : std::runtime_error(_get_trace()) {}

private:
  std::string _get_trace() {
    std::ostringstream ss;

    backward::StackTrace stackTrace;
    backward::TraceResolver resolver;
    stackTrace.load_here();
    resolver.load_stacktrace(stackTrace);

    for (std::size_t i = 0; i < stackTrace.size(); ++i) {
      const backward::ResolvedTrace trace = resolver.resolve(stackTrace[i]);

      ss << "#" << i << " ";
      
      // Print object/function name
      if (!trace.object_function.empty()) {
        ss << trace.object_function;
      } else {
        ss << "0x" << std::hex << stackTrace[i].addr << std::dec;
      }
      
      // Print source file and line number if available
      if (!trace.source.filename.empty()) {
        ss << "\n     at " << trace.source.filename;
        if (trace.source.line > 0) {
          ss << ":" << trace.source.line;
        }
        if (trace.source.col > 0) {
          ss << ":" << trace.source.col;
        }
      }
      
      // Print object file if available
      if (!trace.object_filename.empty()) {
        ss << "\n     in " << trace.object_filename;
      }
      
      ss << "\n";
    }

    return ss.str();
  }
};

void f(int i) {
  if (i >= 42) {
    throw TracedException();
  } else {
    std::cout << "i=" << i << "\n";
    f(i + 1);
  }
}

// Simple exception class - the most basic exception you can create
class FooException : public std::exception {
public:
  // Override the what() method to provide an error message
  const char* what() const noexcept override {
    return "FooException occurred!";
  }
};

int foo() {
  // Function that throws our simple exception
  throw FooException();
}

int main() {
  try {
    f(0);
  } catch (const TracedException &ex) {
    std::cout << ex.what();
  }

  foo();
  
  return 0;
}