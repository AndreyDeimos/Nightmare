#include "tcp-proxy.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <print>

int main() {
  try {
    boost::asio::io_context ctx;

    auto listen_endpoint =
        *boost::asio::ip::tcp::resolver(ctx)
             .resolve("0.0.0.0", "1080",
                      boost::asio::ip::tcp::resolver::passive)
             .begin();

    auto target_endpoint = *boost::asio::ip::tcp::resolver(ctx)
                                .resolve("tcpbin.com", "4242")
                                .begin();

    boost::asio::ip::tcp::acceptor acceptor(ctx, listen_endpoint);

    boost::asio::co_spawn(ctx, tcp_proxy::listen(acceptor, target_endpoint),
                          boost::asio::detached);

    ctx.run();
  } catch (std::exception &e) {
    std::println("Exception: {0}", e.what());
  }
}
