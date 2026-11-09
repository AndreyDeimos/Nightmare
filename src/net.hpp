#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <variant>

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

  boost::asio::awaitable<void> read_exact(boost::asio::mutable_buffer buf);
  boost::asio::awaitable<void> write_exact(boost::asio::const_buffer buf);
};

struct TransportRaw : Transport {
  boost::asio::ip::tcp::socket socket;
  explicit TransportRaw(boost::asio::ip::tcp::socket &&socket_);
  virtual boost::asio::awaitable<size_t>
  async_write_some(boost::asio::const_buffer buf);
  virtual boost::asio::awaitable<size_t>
  async_read_some(boost::asio::mutable_buffer buf);
  virtual boost::asio::awaitable<void> shutdown_send();
  static boost::asio::awaitable<std::unique_ptr<TransportRaw>>
  create(boost::asio::ip::tcp::socket socket_);
};

struct TransportTLS : Transport {
  std::shared_ptr<boost::asio::ssl::context> ctx;
  boost::asio::ssl::stream<boost::asio::ip::tcp::socket> socket;

  TransportTLS(boost::asio::ip::tcp::socket &&socket_,
               std::shared_ptr<boost::asio::ssl::context> ctx_);
  virtual boost::asio::awaitable<size_t>
  async_write_some(boost::asio::const_buffer buf);
  virtual boost::asio::awaitable<size_t>
  async_read_some(boost::asio::mutable_buffer buf);
  virtual boost::asio::awaitable<void> shutdown_send();

  boost::asio::awaitable<void> handshake();

  static boost::asio::awaitable<std::unique_ptr<TransportTLS>>
  create(boost::asio::ip::tcp::socket socket_,
         std::shared_ptr<boost::asio::ssl::context> ctx);
};

boost::asio::awaitable<boost::asio::ip::tcp::socket> dial(dest &target);

boost::asio::awaitable<void> pump(Transport &from, Transport &to);

boost::asio::awaitable<void> relay(std::unique_ptr<Transport> client,
                                   std::unique_ptr<Transport> upstream);
