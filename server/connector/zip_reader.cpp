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

#include "connector/zip_reader.hpp"

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <string>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace sdb::connector::zip {
namespace {

struct EntryInfo {
  uint64_t size;
  uint64_t compressed_size;
  uint64_t data_offset;
  uint32_t crc;
  uint16_t method;
};

EntryInfo FindEntry(Source& source, std::string_view member);

constexpr size_t kChunk = 64 * 1024;
constexpr uint32_t kMax32 = UINT32_MAX;

void InvalidException(const char* message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR), ERR_MSG(message));
}

uint64_t GetNumber(std::span<const std::byte> bytes, size_t offset,
                   size_t width) {
  if (offset > bytes.size() || width > bytes.size() - offset) {
    InvalidException("Truncated ZIP header");
  }

  switch (width) {
    case 2:
      return absl::little_endian::Load16(bytes.data() + offset);
    case 4:
      return absl::little_endian::Load32(bytes.data() + offset);
    case 8:
      return absl::little_endian::Load64(bytes.data() + offset);
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("Unsupported ZIP integer width: ", width));
  }
}

void CheckRange(uint64_t offset, uint64_t length, uint64_t end) {
  if (offset > end || length > end - offset) {
    InvalidException("ZIP range exceeds file bounds");
  }
}

void ValidateName(std::string_view name) {
  if (name.empty() || name.front() == '/' || name.find('\0') != name.npos ||
      name.find('\\') != name.npos || name.find(':') != name.npos) {
    InvalidException("Unsafe or unsupported ZIP member name");
  }
  bool has_component = false;
  while (!name.empty()) {
    const auto slash = name.find('/');
    const auto part = name.substr(0, slash);
    if (part == "..") {
      InvalidException("ZIP member traversal is not allowed");
    }
    has_component |= !part.empty() && part != ".";
    if (slash == name.npos) {
      break;
    }
    name.remove_prefix(slash + 1);
  }
  if (!has_component) {
    InvalidException("Empty ZIP member name");
  }
}

std::string Normalize(std::string_view name) {
  ValidateName(name);
  std::string result;
  while (!name.empty()) {
    auto slash = name.find('/');
    auto part = name.substr(0, slash);
    if (!part.empty() && part != ".") {
      if (!result.empty()) {
        result += '/';
      }
      result.append(part);
    }
    if (slash == name.npos) {
      break;
    }
    name.remove_prefix(slash + 1);
  }
  return result;
}

void Zip64(std::span<const std::byte> extra, uint64_t& size,
           uint64_t& compressed, uint64_t& offset, uint64_t& disk) {
  bool found = false;
  size_t pos = 0;
  while (pos < extra.size()) {
    auto type = GetNumber(extra, pos, 2);
    auto length = GetNumber(extra, pos + 2, 2);
    pos += 4;
    CheckRange(pos, length, extra.size());
    if (type == 1) {
      if (found) {
        InvalidException("Duplicate ZIP64 extra field");
      }
      found = true;
      auto values = extra.subspan(pos, length);
      size_t cursor = 0;
      for (auto* value : {&size, &compressed, &offset}) {
        if (*value == kMax32) {
          *value = GetNumber(values, cursor, 8);
          cursor += 8;
        }
      }
      if (disk == UINT16_MAX) {
        disk = GetNumber(values, cursor, 4);
      }
    }
    pos += length;
  }
  if (!found && (size == kMax32 || compressed == kMax32 || offset == kMax32 ||
                 disk == UINT16_MAX)) {
    InvalidException("Missing ZIP64 extra field");
  }
}

bool NameLess(std::string_view lhs, std::string_view rhs) {
  while (true) {
    const auto lslash = lhs.find('/');
    const auto rslash = rhs.find('/');
    const auto lpart = lhs.substr(0, lslash);
    const auto rpart = rhs.substr(0, rslash);
    if (lpart != rpart) {
      return lpart < rpart;
    }
    if (lslash == lhs.npos || rslash == rhs.npos) {
      return lslash == lhs.npos && rslash != rhs.npos;
    }
    lhs.remove_prefix(lslash + 1);
    rhs.remove_prefix(rslash + 1);
  }
}

class CentralDirectory {
 public:
  CentralDirectory(Source& source, uint64_t offset, uint64_t size)
    : _source{source}, _offset{offset}, _size{size} {
    if (size <= kMetadataCacheLimit) {
      _cache.resize(size);
      for (size_t cursor = 0; cursor < _cache.size(); cursor += kChunk) {
        const auto count = std::min(kChunk, _cache.size() - cursor);
        _source.Check();
        _source.ReadAt(offset + cursor,
                       std::span{_cache}.subspan(cursor, count));
      }
    }
  }

  void Read(uint64_t offset, std::span<std::byte> output) {
    _source.Check();
    CheckRange(offset, output.size(), _offset + _size);
    if (offset < _offset) {
      InvalidException("ZIP central directory read precedes directory");
    }
    if (!_cache.empty()) {
      std::memcpy(output.data(), _cache.data() + (offset - _offset),
                  output.size());
    } else {
      _source.ReadAt(offset, output);
    }
  }

  void Add(uint64_t offset) { _entries.push_back(offset); }

  void ValidateNames() {
    std::sort(_entries.begin(), _entries.end(),
              [&](uint64_t lhs, uint64_t rhs) {
                return NameLess(NameAt(lhs).first, NameAt(rhs).first);
              });
    std::string previous;
    bool previous_directory = true;
    for (auto offset : _entries) {
      auto [name, directory] = NameAt(offset);
      if (name == previous ||
          (!previous_directory && name.starts_with(previous) &&
           name.size() > previous.size() && name[previous.size()] == '/')) {
        InvalidException("Duplicate ZIP name or file/directory conflict");
      }
      previous = std::move(name);
      previous_directory = directory;
    }
  }

 private:
  std::pair<std::string, bool> NameAt(uint64_t offset) {
    std::array<std::byte, 46> header;
    Read(offset, header);
    std::vector<std::byte> name_bytes(GetNumber(header, 28, 2));
    Read(offset + 46, name_bytes);
    const std::string_view raw{reinterpret_cast<const char*>(name_bytes.data()),
                               name_bytes.size()};
    auto name = Normalize(raw);
    const auto attributes = GetNumber(header, 38, 4);
    const bool directory = raw.back() == '/' || (attributes & 0x10) ||
                           ((attributes >> 16) & 0170000) == 0040000;
    return {std::move(name), directory};
  }

  Source& _source;
  const uint64_t _offset;
  const uint64_t _size;
  std::vector<std::byte> _cache;
  std::vector<uint64_t> _entries;
};

}  // namespace

Path ParsePath(std::string_view path) {
  if (!path.starts_with("zip://")) {
    InvalidException("ZIP path must start with zip://");
  }
  path.remove_prefix(6);
  if (path.find_first_of("*?[") != path.npos) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("ZIP glob patterns are not supported yet"));
  }
  const auto split = path.find(".zip");
  if (split == path.npos || split + 4 >= path.size() ||
      path[split + 4] != '/') {
    InvalidException("Expected zip://archive.zip/member path");
  }
  Path result{path.substr(0, split + 4), path.substr(split + 5)};
  if (result.archive.find(':') != path.npos ||
      result.archive.find('\0') != path.npos ||
      result.archive.find('\\') != path.npos ||
      result.archive.starts_with("//")) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("ZIP supports only local archive files"));
  }
  if (result.member.empty() || result.member.back() == '/') {
    InvalidException(
      "ZIP path must select one file, not an archive or directory");
  }
  ValidateName(result.member);
  return result;
}

Reader::Reader(Source& source, std::string_view member) : _source{source} {
  const auto entry = FindEntry(source, member);
  _size = entry.size;
  _compressed_size = entry.compressed_size;
  _data_offset = entry.data_offset;
  _expected_crc = entry.crc;
  _method = entry.method;
}

Reader::~Reader() noexcept { Stop(); }

namespace {

EntryInfo FindEntry(Source& source, std::string_view member) {
  EntryInfo entry_info{};
  source.Check();
  const auto file_size = source.Size();
  const auto wanted = Normalize(member);
  std::vector<std::byte> tail;
  tail.resize(std::min<uint64_t>(file_size, 65535 + 22));
  source.ReadAt(file_size - tail.size(), tail);
  size_t eocd = tail.size();
  if (tail.size() >= 22) {
    for (size_t i = tail.size() - 22 + 1; i-- > 0;) {
      if (GetNumber(tail, i, 4) == 0x06054b50 &&
          GetNumber(tail, i + 20, 2) == tail.size() - i - 22) {
        eocd = i;
        break;
      }
    }
  }
  if (eocd == tail.size()) {
    InvalidException("ZIP end of central directory not found");
  }
  if (GetNumber(tail, eocd + 4, 2) || GetNumber(tail, eocd + 6, 2) ||
      GetNumber(tail, eocd + 8, 2) != GetNumber(tail, eocd + 10, 2)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Multi-disk ZIP is not supported"));
  }
  uint64_t count = GetNumber(tail, eocd + 10, 2);
  uint64_t cd_size = GetNumber(tail, eocd + 12, 4);
  uint64_t cd_offset = GetNumber(tail, eocd + 16, 4);
  uint64_t cd_bound = file_size - tail.size() + eocd;
  if (count == UINT16_MAX || cd_size == kMax32 || cd_offset == kMax32) {
    if (cd_bound < 20) {
      InvalidException("Missing ZIP64 locator");
    }
    std::array<std::byte, 20> locator;
    source.ReadAt(cd_bound - 20, locator);
    if (GetNumber(locator, 0, 4) != 0x07064b50 || GetNumber(locator, 4, 4) ||
        GetNumber(locator, 16, 4) != 1) {
      InvalidException("Invalid or multi-disk ZIP64 locator");
    }
    auto zip64_offset = GetNumber(locator, 8, 8);
    CheckRange(zip64_offset, 56, cd_bound - 20);
    std::array<std::byte, 56> header;
    source.ReadAt(zip64_offset, header);
    auto length = GetNumber(header, 4, 8);
    if (GetNumber(header, 0, 4) != 0x06064b50 || length < 44 ||
        GetNumber(header, 16, 4) || GetNumber(header, 20, 4) ||
        GetNumber(header, 24, 8) != GetNumber(header, 32, 8)) {
      InvalidException("Invalid ZIP64 end record");
    }
    CheckRange(zip64_offset + 12, length, cd_bound - 20);
    if (zip64_offset + 12 + length != cd_bound - 20) {
      InvalidException("Invalid ZIP64 end record length");
    }
    count = GetNumber(header, 32, 8);
    cd_size = GetNumber(header, 40, 8);
    cd_offset = GetNumber(header, 48, 8);
    cd_bound = zip64_offset;
  }
  CheckRange(cd_offset, cd_size, cd_bound);
  if (count > cd_size / 46) {
    InvalidException("Invalid ZIP entry count");
  }
  CentralDirectory directory_index{source, cd_offset, cd_size};
  std::vector<std::byte> variable;
  std::string selected_name;
  uint64_t selected_offset = 0;
  uint16_t selected_flags = 0;
  bool found = false;
  uint64_t cursor = cd_offset;
  for (uint64_t i = 0; i < count; ++i) {
    source.Check();
    CheckRange(cursor, 46, cd_offset + cd_size);
    std::array<std::byte, 46> header;
    directory_index.Read(cursor, header);
    if (GetNumber(header, 0, 4) != 0x02014b50) {
      InvalidException("Invalid ZIP central directory header");
    }
    auto name_size = GetNumber(header, 28, 2);
    auto extra_size = GetNumber(header, 30, 2);
    auto comment_size = GetNumber(header, 32, 2);
    CheckRange(cursor + 46, name_size + extra_size + comment_size,
               cd_offset + cd_size);
    variable.resize(name_size + extra_size);
    directory_index.Read(cursor + 46, variable);
    std::string_view raw{reinterpret_cast<const char*>(variable.data()),
                         name_size};
    auto name = Normalize(raw);
    const auto flags = GetNumber(header, 8, 2);
    const auto attributes = GetNumber(header, 38, 4);
    const auto unix_type = (attributes >> 16) & 0170000;
    bool directory =
      raw.back() == '/' || (attributes & 0x10) || unix_type == 0040000;
    if (unix_type && unix_type != 0100000 && unix_type != 0040000) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("ZIP symlink and special entries are not supported"));
    }
    directory_index.Add(cursor);
    uint64_t size = GetNumber(header, 24, 4);
    uint64_t compressed = GetNumber(header, 20, 4);
    uint64_t offset = GetNumber(header, 42, 4);
    uint64_t disk = GetNumber(header, 34, 2);
    Zip64(std::span{variable}.subspan(name_size), size, compressed, offset,
          disk);
    if (disk) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Multi-disk ZIP is not supported"));
    }
    CheckRange(offset, 30, cd_offset);
    if (name == wanted) {
      if (directory) {
        InvalidException("ZIP member is a directory");
      }
      if (flags & (1 | 32 | 64 | 8192)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("Encrypted or patched ZIP entries are not supported"));
      }
      entry_info.method = GetNumber(header, 10, 2);
      if (entry_info.method != kStore && entry_info.method != kDeflate) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("Unsupported ZIP compression method"));
      }
      if (size > INT64_MAX || compressed > INT64_MAX) {
        InvalidException("ZIP member size exceeds supported range");
      }
      entry_info.size = size;
      entry_info.compressed_size = compressed;
      entry_info.crc = GetNumber(header, 16, 4);
      selected_offset = offset;
      selected_flags = flags;
      selected_name.assign(raw);
      found = true;
    }
    cursor += 46 + name_size + extra_size + comment_size;
  }
  if (cursor != cd_offset + cd_size) {
    InvalidException("ZIP central directory size mismatch");
  }
  directory_index.ValidateNames();
  if (!found) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FILE),
                    ERR_MSG("ZIP member not found"));
  }
  std::array<std::byte, 30> local;
  source.ReadAt(selected_offset, local);
  if (GetNumber(local, 0, 4) != 0x04034b50 ||
      GetNumber(local, 6, 2) != selected_flags ||
      GetNumber(local, 8, 2) != entry_info.method) {
    InvalidException("ZIP local header disagrees with central directory");
  }
  auto name_size = GetNumber(local, 26, 2);
  auto extra_size = GetNumber(local, 28, 2);
  CheckRange(selected_offset + 30, name_size + extra_size, cd_offset);
  variable.resize(name_size + extra_size);
  source.ReadAt(selected_offset + 30, variable);
  if (std::string_view{reinterpret_cast<const char*>(variable.data()),
                       name_size} != selected_name) {
    InvalidException("ZIP local filename disagrees with central directory");
  }
  entry_info.data_offset = selected_offset + 30 + name_size + extra_size;
  CheckRange(entry_info.data_offset, entry_info.compressed_size, cd_offset);
  uint64_t local_size = GetNumber(local, 22, 4);
  uint64_t local_compressed = GetNumber(local, 18, 4);
  uint64_t unused_offset = 0;
  uint64_t unused_disk = 0;
  bool descriptor64 = local_size == kMax32 || local_compressed == kMax32 ||
                      entry_info.size >= kMax32 ||
                      entry_info.compressed_size >= kMax32;
  Zip64(std::span{variable}.subspan(name_size), local_size, local_compressed,
        unused_offset, unused_disk);
  if (!(selected_flags & 8)) {
    if (local_size != entry_info.size ||
        local_compressed != entry_info.compressed_size ||
        GetNumber(local, 14, 4) != entry_info.crc) {
      InvalidException(
        "ZIP local sizes or CRC disagree with central directory");
    }
  } else {
    if ((local_size && local_size != entry_info.size) ||
        (local_compressed && local_compressed != entry_info.compressed_size) ||
        (GetNumber(local, 14, 4) &&
         GetNumber(local, 14, 4) != entry_info.crc)) {
      InvalidException("ZIP local header disagrees with data descriptor");
    }
    const auto descriptor_offset =
      entry_info.data_offset + entry_info.compressed_size;
    std::array<std::byte, 24> descriptor;
    const size_t width = descriptor64 ? 8 : 4;
    CheckRange(descriptor_offset, 4 + 2 * width, cd_offset);
    const auto available =
      std::min<uint64_t>(descriptor.size(), cd_offset - descriptor_offset);
    source.ReadAt(descriptor_offset, std::span{descriptor}.first(available));
    const auto matches = [&](size_t prefix) {
      return prefix + 4 + 2 * width <= available &&
             GetNumber(descriptor, prefix, 4) == entry_info.crc &&
             GetNumber(descriptor, prefix + 4, width) ==
               entry_info.compressed_size &&
             GetNumber(descriptor, prefix + 4 + width, width) ==
               entry_info.size;
    };
    if (!matches(0) &&
        !(GetNumber(descriptor, 0, 4) == 0x08074b50 && matches(4))) {
      InvalidException("ZIP data descriptor disagrees with central directory");
    }
  }
  if (entry_info.method == kStore &&
      entry_info.size != entry_info.compressed_size) {
    InvalidException("Stored ZIP member has inconsistent sizes");
  }
  return entry_info;
}

}  // namespace

void Reader::Stop() noexcept {
  if (_started) {
    inflateEnd(&_stream);
    _started = false;
  }
}

void Reader::Start() {
  Stop();
  _input.resize(kChunk);
  _discard.resize(kChunk);
  _stream = {};
  const int status = inflateInit2(&_stream, -MAX_WBITS);
  if (status != Z_OK) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("ZIP decoder allocation failed"));
  }
  _started = true;
  _decoded = 0;
  _loaded = 0;
  _crc = 0;
  _ended = false;
  _verified = false;
}

void Reader::CheckState() {
  if (_failed) {
    InvalidException("ZIP reader is in failed state; reopen the file");
  }
  _source.Check();
}

size_t Reader::Inflate(std::span<std::byte> output) {
  _source.Check();
  if (!_stream.avail_in && _loaded < _compressed_size) {
    const auto count = std::min<uint64_t>(kChunk, _compressed_size - _loaded);
    _source.ReadAt(_data_offset + _loaded, std::span{_input}.first(count));
    _loaded += count;
    _stream.next_in = reinterpret_cast<Bytef*>(_input.data());
    _stream.avail_in = count;
  }
  const auto count = std::min(output.size(), kChunk);
  _stream.next_out = reinterpret_cast<Bytef*>(output.data());
  _stream.avail_out = count;
  const auto input_before = _stream.avail_in;
  const auto status = inflate(&_stream, Z_NO_FLUSH);
  if (status == Z_MEM_ERROR) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("ZIP decoder allocation failed"));
  }
  if (status != Z_OK && status != Z_STREAM_END) {
    InvalidException("Invalid or truncated ZIP DEFLATE stream");
  }
  const size_t produced = count - _stream.avail_out;
  if (produced > _size - _decoded) {
    InvalidException("ZIP decompressed size exceeds declared size");
  }
  _decoded += produced;
  _crc = crc32(_crc, reinterpret_cast<const Bytef*>(output.data()), produced);
  _ended = status == Z_STREAM_END;
  if (!_ended && !produced && input_before == _stream.avail_in) {
    InvalidException("ZIP DEFLATE stream made no progress");
  }
  if (_ended &&
      (_decoded != _size || _loaded - _stream.avail_in != _compressed_size ||
       _crc != _expected_crc)) {
    InvalidException("ZIP stream size or CRC mismatch");
  }
  _verified = _ended;
  return produced;
}

void Reader::Advance(uint64_t target) {
  if (!_started || target < _decoded) {
    Start();
  }
  while (_decoded < target) {
    const auto count = std::min<uint64_t>(_discard.size(), target - _decoded);
    Inflate(std::span{_discard}.first(count));
    if (_ended && _decoded < target) {
      InvalidException("Unexpected ZIP EOF");
    }
  }
}

void Reader::Finish() {
  if (_method == kDeflate && !_verified) {
    if (!_started) {
      Start();
    }
    std::array<std::byte, 1> probe;
    while (!_ended) {
      Inflate(probe);
    }
  } else if (_method == kStore && _crc_valid && _crc_position == _size &&
             _crc != _expected_crc) {
    InvalidException("ZIP CRC mismatch");
  }
}

size_t Reader::Read(std::span<std::byte> output) {
  try {
    CheckState();
    if (output.empty()) {
      return 0;
    }
    size_t count = std::min<uint64_t>(output.size(), _size - _position);
    if (_method == kDeflate) {
      Advance(_position);
      size_t done = 0;
      while (done < count) {
        done += Inflate(output.subspan(done, count - done));
      }
    } else {
      if (_position != _crc_position) {
        _crc_valid = false;
      }
      for (size_t done = 0; done < count;) {
        _source.Check();
        auto part = output.subspan(done, std::min(kChunk, count - done));
        _source.ReadAt(_data_offset + _position + done, part);
        if (_crc_valid) {
          _crc = crc32(_crc, reinterpret_cast<const Bytef*>(part.data()),
                       part.size());
          _crc_position += part.size();
        }
        done += part.size();
      }
    }
    _position += count;
    if (_position == _size) {
      Finish();
    }
    return count;
  } catch (...) {
    _failed = true;
    throw;
  }
}

void Reader::Seek(uint64_t offset) {
  CheckState();
  CheckRange(offset, 0, _size);
  _position = offset;
  if (!offset && _method == kStore) {
    _crc_position = 0;
    _crc_valid = true;
    _crc = 0;
  }
}

void Reader::ReadAt(uint64_t offset, std::span<std::byte> output) {
  CheckRange(offset, output.size(), _size);
  Seek(offset);
  Read(output);
}

}  // namespace sdb::connector::zip
