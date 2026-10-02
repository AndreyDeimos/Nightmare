#include <algorithm>
#include <array>
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/impl/co_spawn.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/address_v6.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/ssl/stream_base.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <print>
#include <string>
#include <variant>

using tcp = boost::asio::ip::tcp;
namespace ssl = boost::asio::ssl;
using namespace boost::asio::experimental::awaitable_operators;

// https://www.boost.org/doc/libs/latest/doc/html/boost_asio/example/cpp20/coroutines/echo_server_with_as_tuple_default.cpp

struct dest {
  std::variant<boost::asio::ip::address, std::string> host;
  uint16_t port;
};

struct Transport {
  virtual ~Transport() = default;
  virtual boost::asio::awaitable<size_t>
  async_write_some(boost::asio::const_buffer buf) = 0;
  virtual boost::asio::awaitable<size_t>
  async_read_some(boost::asio::mutable_buffer buf) = 0;
  virtual boost::asio::awaitable<void> shutdown_send() = 0;

  boost::asio::awaitable<void> read_exact(boost::asio::mutable_buffer buf) {
    int got = 0;
    while (got < buf.size()) {
      int n = co_await async_read_some(buf + got);
      if (n == 0)
        throw boost::system::system_error(boost::asio::error::eof);
      got += n;
    }
  }
  boost::asio::awaitable<void> write_exact(boost::asio::const_buffer buf) {
    int sent = 0;
    while (sent < buf.size()) {
      int n = co_await async_write_some(buf + sent);
      if (n == 0)
        throw boost::system::system_error(boost::asio::error::eof);
      sent += n;
    }
  }
};

struct TransportRaw : Transport {
  tcp::socket socket;
  boost::asio::awaitable<size_t>
  async_write_some(boost::asio::const_buffer buf) {
    co_return co_await socket.async_write_some(buf, boost::asio::use_awaitable);
  }
  boost::asio::awaitable<size_t>
  async_read_some(boost::asio::mutable_buffer buf) {
    co_return co_await socket.async_read_some(buf, boost::asio::use_awaitable);
  }
  boost::asio::awaitable<void> shutdown_send() {
    socket.shutdown(tcp::socket::shutdown_send);
    co_return;
  }
  explicit TransportRaw(tcp::socket &&socket_) : socket(std::move(socket_)) {};
  static boost::asio::awaitable<std::unique_ptr<TransportRaw>>
  create(tcp::socket socket_) {
    auto t = std::make_unique<TransportRaw>(std::move(socket_));
    co_return t;
  }
};

struct TransportTLS : Transport {
  ssl::context ctx;
  ssl::stream<tcp::socket> socket;

  TransportTLS(tcp::socket &&socket_)
      : ctx(make_ctx()), socket(std::move(socket_), ctx) {}

  static ssl::context make_ctx() {
    ssl::context ctx{ssl::context::tls_server};
    ctx.use_certificate_chain_file("server.pem");
    ctx.use_private_key_file("server.pem", ssl::context::pem);
    return ctx;
  }

  boost::asio::awaitable<void> handshake() {
    co_await socket.async_handshake(ssl::stream_base::server,
                                    boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> shutdown_send() {
    co_await socket.async_shutdown(boost::asio::use_awaitable);
  }

  boost::asio::awaitable<size_t>
  async_read_some(boost::asio::mutable_buffer buf) {
    co_return co_await socket.async_read_some(buf, boost::asio::use_awaitable);
  }

  boost::asio::awaitable<size_t>
  async_write_some(boost::asio::const_buffer buf) {
    co_return co_await socket.async_write_some(buf, boost::asio::use_awaitable);
  }

  static boost::asio::awaitable<std::unique_ptr<TransportTLS>>
  create(tcp::socket socket_) {
    auto t = std::make_unique<TransportTLS>(std::move(socket_));
    co_await t->handshake();
    co_return t;
  }
};

boost::asio::awaitable<tcp::socket> dial(dest &target) {
  auto ex = co_await boost::asio::this_coro::executor;
  tcp::socket socket(ex);

  co_await std::visit(
      [&](auto &&host) -> boost::asio::awaitable<void> {
        if constexpr (std::is_same_v<std::decay_t<decltype(host)>,
                                     boost::asio::ip::address>) {
          co_await socket.async_connect({host, target.port},
                                        boost::asio::use_awaitable);
        } else {
          tcp::resolver res(ex);
          auto results = co_await res.async_resolve(
              host, std::to_string(target.port), boost::asio::use_awaitable);
          co_await boost::asio::async_connect(socket, results,
                                              boost::asio::use_awaitable);
        }
      },
      target.host);

  co_return socket;
}

boost::asio::awaitable<void> pump(Transport &from, Transport &to) {
  std::array<std::byte, 4096> buf;
  try {
    for (;;) {
      size_t n = co_await from.async_read_some(boost::asio::buffer(buf));
      if (n == 0)
        break;
      co_await to.write_exact(boost::asio::buffer(buf, n));
    }
  } catch (boost::system::system_error &e) {
    if (e.code() != boost::asio::error::eof &&
        e.code() != ssl::error::stream_truncated)
      throw;
  }
  co_await to.shutdown_send();
}

boost::asio::awaitable<void> relay(std::unique_ptr<Transport> client,
                                   std::unique_ptr<Transport> upstream) {
  co_await (pump(*client, *upstream) && pump(*upstream, *client));
}

namespace vless {

enum Command { TCP = 1, UDP = 2, MUX = 3 };

struct ProtoInit {
  dest target;
  std::array<uint8_t, 16> user;
  Command command;
  std::string flow;
};

boost::asio::awaitable<ProtoInit> handshake(Transport &transport) {
  ProtoInit req;

  std::array<uint8_t, 18> head;
  co_await transport.read_exact(boost::asio::buffer(head));
  std::copy_n(head.begin() + 1, 16, req.user.begin());

  int addon_len = head[17];
  if (addon_len) {
    std::vector<std::byte> scratch(addon_len);
    co_await transport.read_exact(boost::asio::buffer(scratch));
  }

  std::array<uint8_t, 4> cpt;
  co_await transport.read_exact(boost::asio::buffer(cpt));
  if (cpt[0] < 1 || cpt[0] > 3 || cpt[3] < 1 || cpt[3] > 3) {
    throw boost::system::system_error(boost::asio::error::eof);
  }
  req.command = (Command)cpt[0];
  req.target.port = cpt[1] << 8 | cpt[2];
  uint8_t address_type = cpt[3];

  switch (address_type) {
  case 1: {
    boost::asio::ip::address_v4::bytes_type b;
    co_await transport.read_exact(boost::asio::buffer(b));
    req.target.host = boost::asio::ip::address_v4(b);
    break;
  }
  case 2: {
    uint8_t len = 0;
    co_await transport.read_exact(boost::asio::buffer(&len, 1));
    std::string host(len, '\0');
    co_await transport.read_exact(boost::asio::buffer(host));
    req.target.host = std::move(host);
    break;
  }
  case 3: {
    boost::asio::ip::address_v6::bytes_type b;
    co_await transport.read_exact(boost::asio::buffer(b));
    req.target.host = boost::asio::ip::address_v6(b);
    break;
  }
  default:
    throw boost::system::system_error(boost::asio::error::eof);
  }
  co_await transport.write_exact(boost::asio::buffer(
      std::array<std::byte, 2>{(std::byte)head[0], std::byte(0)}));
  co_return req;
}

boost::asio::awaitable<void> listener(tcp::socket socket) {
  auto t = co_await TransportTLS::create(std::move(socket));
  ProtoInit protoinit = co_await handshake(*t);
  // TODO: auth
  if (protoinit.command != Command::TCP) {
    throw boost::system::system_error(
        boost::asio::error::operation_not_supported);
  }

  tcp::socket upstream = co_await dial(protoinit.target);
  auto upstream_transport = co_await TransportRaw::create(std::move(upstream));

  co_await relay(std::move(t), std::move(upstream_transport));
}
} // namespace vless

boost::asio::awaitable<void> listener(tcp::acceptor &acceptor) {
  for (;;) {
    auto [e, socket] = co_await acceptor.async_accept(boost::asio::as_tuple);
    if (e) {
      std::println("Failed on getting a socket: {0}", e.to_string());
      co_return;
    }
    if (socket.is_open()) {
      boost::asio::co_spawn(co_await boost::asio::this_coro::executor,
                            vless::listener(std::move(socket)),
                            boost::asio::detached);
    }
  }
}

int main() {
  try {
    boost::asio::io_context ctx;
    auto ex = ctx.get_executor();
    tcp::acceptor acceptor(ex);
    tcp::endpoint endpoint(tcp::v4(), 8080);
    acceptor.open(endpoint.protocol());
    acceptor.bind(endpoint);
    acceptor.listen();
    co_spawn(ex, listener(acceptor), boost::asio::detached);
    ctx.run();
  } catch (boost::system::system_error &e) {
    std::println("Error: {0}", e.what());
  }
}
