#include <algorithm>
#include <format>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string_view>
#include <unordered_map>

// =====================================================================
// third-party libraries
// =====================================================================

// spdlog
#include <spdlog/spdlog.h>

namespace play::format {
using CompositeFoo = std::unordered_map<std::string, std::string>;
}  // namespace play::format

template <>
struct std::formatter<play::format::CompositeFoo, char> {
    bool quoted = false;

    template <class ParseContext>
    constexpr ParseContext::iterator parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FmtContext>
    FmtContext::iterator format(play::format::CompositeFoo foo,
                                FmtContext& ctx) const {
        std::ostringstream out;
        out << "{";
        for (const auto& [key, value] : foo) {
            out << std::quoted(key) << ": " << std::quoted(value) << ", ";
        }
        std::string result = out.str();
        result.erase(result.size() - 2);  // Remove the last comma and space
        result += "}";
        return std::ranges::copy(std::move(result), ctx.out()).out;
    }
};

int main() {
    play::format::CompositeFoo foo = {{"key1", "value1"}, {"key2", "value2"}};
    std::cout << std::format("Formatted: {}", foo) << std::endl;
    SPDLOG_INFO("Formatted: {}", foo);
    return 0;
}