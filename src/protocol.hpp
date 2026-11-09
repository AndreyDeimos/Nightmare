#pragma once

#include "net.hpp"
#include <boost/asio/awaitable.hpp>

namespace vless {

enum Command { TCP = 1, UDP = 2, MUX = 3 };

struct ProtoInit {
  dest target;
  std::array<uint8_t, 16> user;
  Command command;
  std::string flow;
};

boost::asio::awaitable<ProtoInit> handshake(Transport &transport);
} // namespace vless

struct HandshakeResult {
  dest target;
  std::optional<vless::ProtoInit> proxy;
};
