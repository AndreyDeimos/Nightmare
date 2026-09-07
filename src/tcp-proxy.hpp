#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace tcp_proxy {
using boost::asio::awaitable;
using boost::asio::ip::tcp;
awaitable<void> listen(tcp::acceptor &acceptor, tcp::endpoint target);
awaitable<void> proxy(tcp::socket client, tcp::endpoint target);
} // namespace tcp_proxy
