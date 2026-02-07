#include <thread>

int main() {
  std::thread worker([]() {});
  worker.join();
  return 0;
}
