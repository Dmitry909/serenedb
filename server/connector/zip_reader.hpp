////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <zlib.h>

#include <array>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace sdb::connector::zip {

enum class ErrorKind : uint8_t {
  Invalid,
  Missing,
  Unsupported,
  Memory,
};

struct Error : std::runtime_error {
  Error(ErrorKind kind, const std::string& message)
    : std::runtime_error{message}, kind{kind} {}
  ErrorKind kind;
};

struct Path {
  std::string_view archive;
  std::string_view member;
};

Path ParsePath(std::string_view path);

class Source {
 public:
  virtual ~Source() = default;
  virtual uint64_t Size() const = 0;
  virtual void ReadAt(uint64_t offset, std::span<std::byte> output) = 0;
  virtual void Check() = 0;
};

inline constexpr size_t kMetadataCacheLimit = 50 * 1024 * 1024;

struct EntryInfo {
  uint64_t size;
  uint64_t compressed_size;
  uint64_t data_offset;
  uint32_t crc;
  uint16_t method;
};

EntryInfo FindEntry(Source& source, std::string_view member);

class Reader {
 public:
  Reader(Source& source, std::string_view member);
  Reader(Source& source, const EntryInfo& entry);
  ~Reader() noexcept;
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  uint64_t Size() const { return _size; }
  uint64_t Position() const { return _position; }
  bool Compressed() const { return _method == 8; }
  size_t Read(std::span<std::byte> output);
  void ReadAt(uint64_t offset, std::span<std::byte> output);
  void Seek(uint64_t offset);

 private:
  void Start();
  void Stop() noexcept;
  void Advance(uint64_t target);
  size_t Inflate(std::span<std::byte> output);
  void Finish();
  void CheckState();

  Source& _source;
  std::vector<std::byte> _input;
  std::vector<std::byte> _discard;
  z_stream _stream{};
  uint64_t _size = 0;
  uint64_t _compressed_size = 0;
  uint64_t _data_offset = 0;
  uint64_t _position = 0;
  uint64_t _decoded = 0;
  uint64_t _loaded = 0;
  uint64_t _crc_position = 0;
  uint32_t _expected_crc = 0;
  uint32_t _crc = 0;
  uint16_t _method = 0;
  bool _started = false;
  bool _ended = false;
  bool _verified = false;
  bool _failed = false;
  bool _crc_valid = true;
};

}  // namespace sdb::connector::zip
