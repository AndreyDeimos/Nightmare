#include <array>
#include <boost/asio.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/executor_work_guard.hpp>
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
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <print>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

#include "net.hpp"
#include "protocol.hpp"

// https://www.boost.org/doc/libs/latest/doc/html/boost_asio/example/cpp20/coroutines/echo_server_with_as_tuple_default.cpp

struct Config {
  bool tls;
  bool vless;
  std::string certificate_chain_file = "server.pem";
  std::string private_key_file = "server.pem";
};

struct Session {
  std::unique_ptr<Transport> transport;
  HandshakeResult handshake;
};

boost::asio::awaitable<void> run_session(
    std::function<boost::asio::awaitable<Session>(boost::asio::ip::tcp::socket)>
        get_inbound,
    std::function<boost::asio::awaitable<std::unique_ptr<Transport>>(dest &)>
        get_outbound,
    boost::asio::ip::tcp::socket client) {
  auto from = co_await get_inbound(std::move(client));
  auto to = co_await get_outbound(from.handshake.target);

  co_await relay(std::move(from.transport), std::move(to));
}

std::function<boost::asio::awaitable<Session>(boost::asio::ip::tcp::socket)>
make_inbound_factory(Config &cfg) {
  if (cfg.tls) {
    auto ctx = std::make_shared<boost::asio::ssl::context>(
        boost::asio::ssl::context::tls_server);
    ctx->use_certificate_chain_file(cfg.certificate_chain_file);
    ctx->use_private_key_file(cfg.private_key_file,
                              boost::asio::ssl::context::pem);
    return [&cfg, ctx](boost::asio::ip::tcp::socket socket_)
               -> boost::asio::awaitable<Session> {
      auto t = co_await TransportTLS::create(std::move(socket_), ctx);
      if (cfg.vless) {
        vless::ProtoInit proxy = co_await vless::handshake(*t);
        HandshakeResult handshake = {proxy.target, proxy};
        co_return Session{std::move(t), handshake};
      } else {
        co_return Session{std::move(t), {{"example.com", 443}, std::nullopt}};
      }
    };
  } else {
    return [&cfg](boost::asio::ip::tcp::socket socket_)
               -> boost::asio::awaitable<Session> {
      auto t = co_await TransportRaw::create(std::move(socket_));
      if (cfg.vless) {
        vless::ProtoInit proxy = co_await vless::handshake(*t);
        co_return Session{std::move(t), {proxy.target, proxy}};
      } else {
        co_return Session{std::move(t), {{"example.com", 443}, std::nullopt}};
      }
    };
  }
}

std::function<boost::asio::awaitable<std::unique_ptr<Transport>>(dest &)>
make_outbound_factory() {
  // TODO: custom outbounds
  return
      [](dest &target) -> boost::asio::awaitable<std::unique_ptr<Transport>> {
        co_return co_await TransportRaw::create(
            std::move(co_await dial(target)));
      };
}

boost::asio::awaitable<void> listener(
    boost::asio::ip::tcp::acceptor &acceptor,
    std::function<boost::asio::awaitable<Session>(boost::asio::ip::tcp::socket)>
        get_inbound,
    std::function<boost::asio::awaitable<std::unique_ptr<Transport>>(dest &)>
        get_outbound) {
  for (;;) {
    auto strand = boost::asio::make_strand(acceptor.get_executor());
    auto [e, socket] = co_await acceptor.async_accept(
        strand, boost::asio::as_tuple(boost::asio::use_awaitable));
    if (e) {
      std::println("Failed on getting a socket: {0}", e.to_string());
      co_return;
    }
    if (socket.is_open()) {
      socket.set_option(boost::asio::ip::tcp::no_delay(true));
      co_spawn(strand,
               run_session(get_inbound, get_outbound, std::move(socket)),
               [](std::exception_ptr e) {
                 if (!e)
                   return;

                 try {
                   std::rethrow_exception(e);
                 } catch (const std::exception &ex) {
                   std::println("session error: {}", ex.what());
                 } catch (...) {
                   std::println("session error: unknown exception");
                 }
               });
    }
  }
}

int main() {
  try {
    boost::asio::io_context ctx;
    auto ex = ctx.get_executor();
    boost::asio::ip::tcp::acceptor acceptor(ex);
    boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(), 8080);
    acceptor.open(endpoint.protocol());
    acceptor.bind(endpoint);
    acceptor.listen();
    Config cfg = {1, 1};
    co_spawn(
        ex,
        listener(acceptor, make_inbound_factory(cfg), make_outbound_factory()),
        [](std::exception_ptr e) {
          if (!e)
            return;

          try {
            std::rethrow_exception(e);
          } catch (const std::exception &ex) {
            std::println("session error: {}", ex.what());
          } catch (...) {
            std::println("session error: unknown exception");
          }
        });
    auto n = std::max(1u, std::thread::hardware_concurrency());
    std::vector<std::jthread> pool;
    pool.reserve(n);
    for (int i = 0; i < n; i++) {
      pool.emplace_back([&ctx](std::stop_token st) {
        std::stop_callback stop_cb(st, [&ctx] { ctx.stop(); });
        ctx.run();
      });
    }
    for (auto &t : pool)
      t.join();
  } catch (boost::system::system_error &e) {
    std::println("Error: {0}", e.what());
  }
}
