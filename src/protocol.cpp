#include "protocol.hpp"

boost::asio::awaitable<vless::ProtoInit>
vless::handshake(Transport &transport) {
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
