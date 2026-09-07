#include "tcp-proxy.hpp"

#include <boost/asio.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#include <array>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <cstddef>
#include <print>
#include <utility>

using boost::asio::as_tuple;
using boost::asio::awaitable;
using boost::asio::buffer;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::steady_timer;
using boost::asio::ip::tcp;
namespace this_coro = boost::asio::this_coro;
using namespace boost::asio::experimental::awaitable_operators;
using namespace std::chrono_literals;

awaitable<void> relay(tcp::socket &from, tcp::socket &to) {
  std::array<std::byte, 4096> data;

  for (;;) {
    auto [ec, n] = co_await from.async_read_some(buffer(data), as_tuple);

    if (ec) {
      if (ec == boost::asio::error::eof) {
        boost::system::error_code ignored;
        std::ignore = to.shutdown(tcp::socket::shutdown_send, ignored);
      }
      co_return;
    }

    auto [wec, written] = co_await async_write(to, buffer(data, n), as_tuple);

    (void)written;

    if (wec)
      co_return;
  }
}

namespace tcp_proxy {
awaitable<void> proxy(tcp::socket client, tcp::endpoint target) {
  std::println("New connection");
  auto ex = client.get_executor();
  tcp::socket server(ex);

  auto [ec] = co_await server.async_connect(target, as_tuple);
  if (ec) {
    std::println("Upstream connection failed: {0}", ec.to_string());
  }

  co_await (relay(client, server) && relay(server, client));
}

awaitable<void> listen(tcp::acceptor &acceptor, tcp::endpoint target) {
  for (;;) {
    auto [e, client] = co_await acceptor.async_accept(as_tuple);
    if (!e) {
      auto ex = client.get_executor();
      co_spawn(ex, proxy(std::move(client), target), detached);
    } else {
      std::println("Accept failed: {0}", e.to_string());
      steady_timer timer(co_await this_coro::executor);
      timer.expires_after(100ms);
      co_await timer.async_wait();
    }
  }
}
} // namespace tcp_proxy
