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

template <typename K, typename V>
struct fmt::formatter<std::unordered_map<K, V>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename Context>
    constexpr auto format(const std::unordered_map<K, V>& map, Context& ctx) const {
        auto out = ctx.out();
        fmt::format_to(out, "{{");
        bool first = true;
        for (const auto& [k, v] : map) {
            if (!first) fmt::format_to(out, ", ");
            fmt::format_to(out, "{}: {}", k, v);
            first = false;
        }
        return fmt::format_to(out, "}}");
    }
};

// template <>
// struct std::formatter<play::format::CompositeFoo, char> {
//     bool quoted = false;

//     template <class ParseContext>
//     constexpr ParseContext::iterator parse(ParseContext& ctx) {
//         return ctx.begin();
//     }

//     template <class FmtContext>
//     FmtContext::iterator format(play::format::CompositeFoo foo,
//                                 FmtContext& ctx) const {
//         std::ostringstream out;
//         out << "{";
//         for (const auto& [key, value] : foo) {
//             out << std::quoted(key) << ": " << std::quoted(value) << ", ";
//         }
//         std::string result = out.str();
//         result.erase(result.size() - 2);  // Remove the last comma and space
//         result += "}";
//         return std::ranges::copy(std::move(result), ctx.out()).out;
//     }
// };

int main() {
    play::format::CompositeFoo foo = {{"key1", "value1"}, {"key2", "value2"}};
    std::cout << fmt::format("Formatted: {}", foo) << std::endl;
    // SPDLOG_INFO("Formatted: {}", foo);
    return 0;
}