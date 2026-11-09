#include "net.hpp"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/use_awaitable.hpp>

using namespace boost::asio::experimental::awaitable_operators;

// Transport

boost::asio::awaitable<void>
Transport::read_exact(boost::asio::mutable_buffer buf) {
  int got = 0;
  while (got < buf.size()) {
    int n = co_await async_read_some(buf + got);
    if (n == 0)
      throw boost::system::system_error(boost::asio::error::eof);
    got += n;
  }
}

boost::asio::awaitable<void>
Transport::write_exact(boost::asio::const_buffer buf) {
  int sent = 0;
  while (sent < buf.size()) {
    int n = co_await async_write_some(buf + sent);
    if (n == 0)
      throw boost::system::system_error(boost::asio::error::eof);
    sent += n;
  }
}

// TransportRaw
boost::asio::awaitable<size_t>
TransportRaw::async_write_some(boost::asio::const_buffer buf) {
  co_return co_await socket.async_write_some(buf, boost::asio::use_awaitable);
}

boost::asio::awaitable<size_t>
TransportRaw::async_read_some(boost::asio::mutable_buffer buf) {
  co_return co_await socket.async_read_some(buf, boost::asio::use_awaitable);
}

boost::asio::awaitable<void> TransportRaw::shutdown_send() {
  socket.shutdown(boost::asio::ip::tcp::socket::shutdown_send);
  co_return;
}

TransportRaw::TransportRaw(boost::asio::ip::tcp::socket &&socket_)
    : socket(std::move(socket_)) {};
boost::asio::awaitable<std::unique_ptr<TransportRaw>>
TransportRaw::create(boost::asio::ip::tcp::socket socket_) {
  auto t = std::make_unique<TransportRaw>(std::move(socket_));
  t->socket.set_option(boost::asio::ip::tcp::no_delay(true));
  co_return t;
}

// TransportTlS

TransportTLS::TransportTLS(boost::asio::ip::tcp::socket &&socket_,
                           std::shared_ptr<boost::asio::ssl::context> ctx_)
    : ctx(ctx_), socket(std::move(socket_), *ctx) {}

boost::asio::awaitable<void> TransportTLS::handshake() {
  co_await socket.async_handshake(boost::asio::ssl::stream_base::server,
                                  boost::asio::use_awaitable);
}

boost::asio::awaitable<void> TransportTLS::shutdown_send() {
  co_await socket.async_shutdown(boost::asio::use_awaitable);
}

boost::asio::awaitable<size_t>
TransportTLS::async_read_some(boost::asio::mutable_buffer buf) {
  co_return co_await socket.async_read_some(buf, boost::asio::use_awaitable);
}

boost::asio::awaitable<size_t>
TransportTLS::async_write_some(boost::asio::const_buffer buf) {
  co_return co_await socket.async_write_some(buf, boost::asio::use_awaitable);
}

boost::asio::awaitable<std::unique_ptr<TransportTLS>>
TransportTLS::create(boost::asio::ip::tcp::socket socket_,
                     std::shared_ptr<boost::asio::ssl::context> ctx) {
  auto t = std::make_unique<TransportTLS>(std::move(socket_), ctx);
  co_await t->handshake();
  co_return t;
}

boost::asio::awaitable<boost::asio::ip::tcp::socket> dial(dest &target) {
  auto ex = co_await boost::asio::this_coro::executor;
  boost::asio::ip::tcp::socket socket(ex);

  co_await std::visit(
      [&](auto &&host) -> boost::asio::awaitable<void> {
        if constexpr (std::is_same_v<std::decay_t<decltype(host)>,
                                     boost::asio::ip::address>) {
          co_await socket.async_connect({host, target.port},
                                        boost::asio::use_awaitable);
        } else {
          boost::asio::ip::tcp::resolver res(ex);
          auto results = co_await res.async_resolve(
              host, std::to_string(target.port), boost::asio::use_awaitable);
          co_await boost::asio::async_connect(socket, results,
                                              boost::asio::use_awaitable);
        }
      },
      target.host);

  socket.set_option(boost::asio::ip::tcp::no_delay(true));
  co_return socket;
}

boost::asio::awaitable<void> pump(Transport &from, Transport &to) {
  std::array<std::byte, 262144> buf;
  try {
    for (;;) {
      size_t n = co_await from.async_read_some(boost::asio::buffer(buf));
      if (n == 0)
        break;
      co_await to.write_exact(boost::asio::buffer(buf, n));
    }
  } catch (boost::system::system_error &e) {
    if (e.code() != boost::asio::error::eof &&
        e.code() != boost::asio::ssl::error::stream_truncated)
      throw;
  }
  co_await to.shutdown_send();
}

boost::asio::awaitable<void> relay(std::unique_ptr<Transport> client,
                                   std::unique_ptr<Transport> upstream) {
  co_await (pump(*client, *upstream) && pump(*upstream, *client));
}
