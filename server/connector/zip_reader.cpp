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

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <string>

namespace sdb::connector::zip {
namespace {

constexpr size_t kChunk = 64 * 1024;
constexpr uint32_t kMax32 = UINT32_MAX;

// Сообщает о повреждённой или недопустимой структуре ZIP.
[[noreturn]] void Invalid(const char* message) {
  throw Error{ErrorKind::Invalid, message};
}

// Читает целое число little-endian из проверенного диапазона байтов.
uint64_t Number(std::span<const std::byte> bytes, size_t offset, size_t width) {
  if (offset > bytes.size() || width > bytes.size() - offset) {
    Invalid("Truncated ZIP header");
  }
  uint64_t value = 0;
  for (size_t i = 0; i < width; ++i) {
    value |= uint64_t{std::to_integer<uint8_t>(bytes[offset + i])} << (i * 8);
  }
  return value;
}

// Проверяет границы диапазона без переполнения при сложении.
void Range(uint64_t offset, uint64_t length, uint64_t end) {
  if (offset > end || length > end - offset) {
    Invalid("ZIP range exceeds file bounds");
  }
}

// Проверяет корректность UTF-8 без выделения дополнительной памяти.
bool Utf8(std::string_view text) {
  size_t pos = 0;
  while (pos < text.size()) {
    const auto first = static_cast<uint8_t>(text[pos++]);
    if (first < 128) {
      continue;
    }
    unsigned count;
    uint32_t code;
    uint32_t minimum;
    if (first >= 0xc2 && first <= 0xdf) {
      count = 1;
      code = first & 31;
      minimum = 128;
    } else if (first >= 0xe0 && first <= 0xef) {
      count = 2;
      code = first & 15;
      minimum = 0x800;
    } else if (first >= 0xf0 && first <= 0xf4) {
      count = 3;
      code = first & 7;
      minimum = 0x10000;
    } else {
      return false;
    }
    while (count--) {
      if (pos == text.size()) {
        return false;
      }
      const auto next = static_cast<uint8_t>(text[pos++]);
      if ((next & 0xc0) != 0x80) {
        return false;
      }
      code = (code << 6) | (next & 63);
    }
    if (code < minimum || code > 0x10ffff ||
        (code >= 0xd800 && code <= 0xdfff)) {
      return false;
    }
  }
  return true;
}

// Отклоняет опасные, пустые и неподдерживаемые имена внутри архива.
void ValidateName(std::string_view name) {
  if (name.empty() || name.front() == '/' || name.find('\0') != name.npos ||
      name.find('\\') != name.npos || name.find(':') != name.npos ||
      !Utf8(name)) {
    Invalid("Unsafe or unsupported ZIP member name");
  }
  bool has_component = false;
  while (!name.empty()) {
    const auto slash = name.find('/');
    const auto part = name.substr(0, slash);
    if (part == "..") {
      Invalid("ZIP member traversal is not allowed");
    }
    has_component |= !part.empty() && part != ".";
    if (slash == name.npos) {
      break;
    }
    name.remove_prefix(slash + 1);
  }
  if (!has_component) {
    Invalid("Empty ZIP member name");
  }
}

// Убирает компоненты «.» и повторные разделители из допустимого имени.
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

// Получает расширенные размеры и смещения из extra field ZIP64.
void Zip64(std::span<const std::byte> extra, uint64_t& size,
           uint64_t& compressed, uint64_t& offset, uint64_t& disk) {
  bool found = false;
  size_t pos = 0;
  while (pos < extra.size()) {
    auto type = Number(extra, pos, 2);
    auto length = Number(extra, pos + 2, 2);
    pos += 4;
    Range(pos, length, extra.size());
    if (type == 1) {
      if (found) {
        Invalid("Duplicate ZIP64 extra field");
      }
      found = true;
      auto values = extra.subspan(pos, length);
      size_t cursor = 0;
      for (auto* value : {&size, &compressed, &offset}) {
        if (*value == kMax32) {
          *value = Number(values, cursor, 8);
          cursor += 8;
        }
      }
      if (disk == UINT16_MAX) {
        disk = Number(values, cursor, 4);
      }
    }
    pos += length;
  }
  if (!found && (size == kMax32 || compressed == kMax32 || offset == kMax32 ||
                 disk == UINT16_MAX)) {
    Invalid("Missing ZIP64 extra field");
  }
}

// Сравнивает компоненты путей так, чтобы каталог и его потомки шли подряд.
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
  // Выбирает хранение каталога в памяти либо чтение с диска по порогу 50 MiB.
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

  // Читает диапазон каталога из кеша или внешнего файла, проверяя отмену.
  void Read(uint64_t offset, std::span<std::byte> output) {
    _source.Check();
    Range(offset, output.size(), _offset + _size);
    if (offset < _offset) {
      Invalid("ZIP central directory read precedes directory");
    }
    if (!_cache.empty()) {
      std::memcpy(output.data(), _cache.data() + (offset - _offset),
                  output.size());
    } else {
      _source.ReadAt(offset, output);
    }
  }

  // Сохраняет смещение очередной записи каталога для проверки имён.
  void Add(uint64_t offset) { _entries.push_back(offset); }

  // Сортирует смещения по именам и отклоняет дубликаты и конфликты каталогов.
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
        Invalid("Duplicate ZIP name or file/directory conflict");
      }
      previous = std::move(name);
      previous_directory = directory;
    }
  }

 private:
  // Читает нормализованное имя и признак каталога по смещению записи.
  std::pair<std::string, bool> NameAt(uint64_t offset) {
    std::array<std::byte, 46> header;
    Read(offset, header);
    std::vector<std::byte> name_bytes(Number(header, 28, 2));
    Read(offset + 46, name_bytes);
    const std::string_view raw{reinterpret_cast<const char*>(name_bytes.data()),
                               name_bytes.size()};
    auto name = Normalize(raw);
    const auto attributes = Number(header, 38, 4);
    const bool directory = raw.back() == '/' || (attributes & 0x10) ||
                           ((attributes >> 16) & 0170000) == 0040000;
    return {std::move(name), directory};
  }

  Source& _source;
  const uint64_t _offset;
  const uint64_t _size;
  std::vector<std::byte> _cache;
  // Для большого каталога храним только смещения, а не все имена.
  std::vector<uint64_t> _entries;
};

}  // namespace

// Разделяет точный локальный путь ZIP на путь архива и имя файла.
Path ParsePath(std::string_view path) {
  if (!path.starts_with("zip://")) {
    Invalid("ZIP path must start with zip://");
  }
  path.remove_prefix(6);
  if (path.find_first_of("*?[") != path.npos) {
    throw Error{ErrorKind::Unsupported,
                "ZIP glob patterns are not supported yet"};
  }
  const auto split = path.find(".zip");
  if (split == path.npos || split + 4 >= path.size() ||
      path[split + 4] != '/') {
    Invalid("Expected zip://archive.zip/member path");
  }
  Path result{path.substr(0, split + 4), path.substr(split + 5)};
  if (result.archive.find(':') != path.npos ||
      result.archive.find('\0') != path.npos ||
      result.archive.find('\\') != path.npos ||
      result.archive.starts_with("//")) {
    throw Error{ErrorKind::Unsupported,
                "ZIP supports only local archive files"};
  }
  if (result.member.empty() || result.member.back() == '/') {
    Invalid("ZIP path must select one file, not an archive or directory");
  }
  ValidateName(result.member);
  return result;
}

// Находит выбранный файл и создаёт reader без распаковки его содержимого.
Reader::Reader(Source& source, std::string_view member)
  : Reader{source, FindEntry(source, member)} {}

// Создаёт reader по уже проверенным размерам, смещению, методу и CRC.
Reader::Reader(Source& source, const EntryInfo& entry)
  : _source{source},
    _size{entry.size},
    _compressed_size{entry.compressed_size},
    _data_offset{entry.data_offset},
    _expected_crc{entry.crc},
    _method{entry.method} {}

// Освобождает декодер без дочитывания файла и проверки оставшегося CRC.
Reader::~Reader() noexcept { Stop(); }

// Проверяет ZIP/ZIP64 и возвращает метаданные единственного выбранного файла.
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
      if (Number(tail, i, 4) == 0x06054b50 &&
          Number(tail, i + 20, 2) == tail.size() - i - 22) {
        eocd = i;
        break;
      }
    }
  }
  if (eocd == tail.size()) {
    Invalid("ZIP end of central directory not found");
  }
  if (Number(tail, eocd + 4, 2) || Number(tail, eocd + 6, 2) ||
      Number(tail, eocd + 8, 2) != Number(tail, eocd + 10, 2)) {
    throw Error{ErrorKind::Unsupported, "Multi-disk ZIP is not supported"};
  }
  uint64_t count = Number(tail, eocd + 10, 2);
  uint64_t cd_size = Number(tail, eocd + 12, 4);
  uint64_t cd_offset = Number(tail, eocd + 16, 4);
  uint64_t cd_bound = file_size - tail.size() + eocd;
  if (count == UINT16_MAX || cd_size == kMax32 || cd_offset == kMax32) {
    if (cd_bound < 20) {
      Invalid("Missing ZIP64 locator");
    }
    std::array<std::byte, 20> locator;
    source.ReadAt(cd_bound - 20, locator);
    if (Number(locator, 0, 4) != 0x07064b50 || Number(locator, 4, 4) ||
        Number(locator, 16, 4) != 1) {
      Invalid("Invalid or multi-disk ZIP64 locator");
    }
    auto zip64_offset = Number(locator, 8, 8);
    Range(zip64_offset, 56, cd_bound - 20);
    std::array<std::byte, 56> header;
    source.ReadAt(zip64_offset, header);
    auto length = Number(header, 4, 8);
    if (Number(header, 0, 4) != 0x06064b50 || length < 44 ||
        Number(header, 16, 4) || Number(header, 20, 4) ||
        Number(header, 24, 8) != Number(header, 32, 8)) {
      Invalid("Invalid ZIP64 end record");
    }
    Range(zip64_offset + 12, length, cd_bound - 20);
    if (zip64_offset + 12 + length != cd_bound - 20) {
      Invalid("Invalid ZIP64 end record length");
    }
    count = Number(header, 32, 8);
    cd_size = Number(header, 40, 8);
    cd_offset = Number(header, 48, 8);
    cd_bound = zip64_offset;
  }
  Range(cd_offset, cd_size, cd_bound);
  if (count > cd_size / 46) {
    Invalid("Invalid ZIP entry count");
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
    Range(cursor, 46, cd_offset + cd_size);
    std::array<std::byte, 46> header;
    directory_index.Read(cursor, header);
    if (Number(header, 0, 4) != 0x02014b50) {
      Invalid("Invalid ZIP central directory header");
    }
    auto name_size = Number(header, 28, 2);
    auto extra_size = Number(header, 30, 2);
    auto comment_size = Number(header, 32, 2);
    Range(cursor + 46, name_size + extra_size + comment_size,
          cd_offset + cd_size);
    variable.resize(name_size + extra_size);
    directory_index.Read(cursor + 46, variable);
    std::string_view raw{reinterpret_cast<const char*>(variable.data()),
                         name_size};
    auto name = Normalize(raw);
    const auto flags = Number(header, 8, 2);
    const auto attributes = Number(header, 38, 4);
    const auto unix_type = (attributes >> 16) & 0170000;
    bool directory =
      raw.back() == '/' || (attributes & 0x10) || unix_type == 0040000;
    if (unix_type && unix_type != 0100000 && unix_type != 0040000) {
      throw Error{ErrorKind::Unsupported,
                  "ZIP symlink and special entries are not supported"};
    }
    directory_index.Add(cursor);
    uint64_t size = Number(header, 24, 4);
    uint64_t compressed = Number(header, 20, 4);
    uint64_t offset = Number(header, 42, 4);
    uint64_t disk = Number(header, 34, 2);
    Zip64(std::span{variable}.subspan(name_size), size, compressed, offset,
          disk);
    if (disk) {
      throw Error{ErrorKind::Unsupported, "Multi-disk ZIP is not supported"};
    }
    Range(offset, 30, cd_offset);
    if (name == wanted) {
      if (directory) {
        Invalid("ZIP member is a directory");
      }
      if (flags & (1 | 32 | 64 | 8192)) {
        throw Error{ErrorKind::Unsupported,
                    "Encrypted or patched ZIP entries are not supported"};
      }
      entry_info.method = Number(header, 10, 2);
      if (entry_info.method != 0 && entry_info.method != 8) {
        throw Error{ErrorKind::Unsupported,
                    "Unsupported ZIP compression method"};
      }
      if (size > INT64_MAX || compressed > INT64_MAX) {
        Invalid("ZIP member size exceeds supported range");
      }
      entry_info.size = size;
      entry_info.compressed_size = compressed;
      entry_info.crc = Number(header, 16, 4);
      selected_offset = offset;
      selected_flags = flags;
      selected_name.assign(raw);
      found = true;
    }
    cursor += 46 + name_size + extra_size + comment_size;
  }
  if (cursor != cd_offset + cd_size) {
    Invalid("ZIP central directory size mismatch");
  }
  directory_index.ValidateNames();
  if (!found) {
    throw Error{ErrorKind::Missing, "ZIP member not found"};
  }
  std::array<std::byte, 30> local;
  source.ReadAt(selected_offset, local);
  if (Number(local, 0, 4) != 0x04034b50 ||
      Number(local, 6, 2) != selected_flags ||
      Number(local, 8, 2) != entry_info.method) {
    Invalid("ZIP local header disagrees with central directory");
  }
  auto name_size = Number(local, 26, 2);
  auto extra_size = Number(local, 28, 2);
  Range(selected_offset + 30, name_size + extra_size, cd_offset);
  variable.resize(name_size + extra_size);
  source.ReadAt(selected_offset + 30, variable);
  if (std::string_view{reinterpret_cast<const char*>(variable.data()),
                       name_size} != selected_name) {
    Invalid("ZIP local filename disagrees with central directory");
  }
  entry_info.data_offset = selected_offset + 30 + name_size + extra_size;
  Range(entry_info.data_offset, entry_info.compressed_size, cd_offset);
  uint64_t local_size = Number(local, 22, 4);
  uint64_t local_compressed = Number(local, 18, 4);
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
        Number(local, 14, 4) != entry_info.crc) {
      Invalid("ZIP local sizes or CRC disagree with central directory");
    }
  } else {
    if ((local_size && local_size != entry_info.size) ||
        (local_compressed && local_compressed != entry_info.compressed_size) ||
        (Number(local, 14, 4) && Number(local, 14, 4) != entry_info.crc)) {
      Invalid("ZIP local header disagrees with data descriptor");
    }
    const auto descriptor_offset =
      entry_info.data_offset + entry_info.compressed_size;
    std::array<std::byte, 24> descriptor;
    const size_t width = descriptor64 ? 8 : 4;
    Range(descriptor_offset, 4 + 2 * width, cd_offset);
    const auto available =
      std::min<uint64_t>(descriptor.size(), cd_offset - descriptor_offset);
    source.ReadAt(descriptor_offset, std::span{descriptor}.first(available));
    const auto matches = [&](size_t prefix) {
      return prefix + 4 + 2 * width <= available &&
             Number(descriptor, prefix, 4) == entry_info.crc &&
             Number(descriptor, prefix + 4, width) ==
               entry_info.compressed_size &&
             Number(descriptor, prefix + 4 + width, width) == entry_info.size;
    };
    // CRC файла может совпадать с необязательной сигнатурой descriptor.
    if (!matches(0) &&
        !(Number(descriptor, 0, 4) == 0x08074b50 && matches(4))) {
      Invalid("ZIP data descriptor disagrees with central directory");
    }
  }
  if (entry_info.method == 0 && entry_info.size != entry_info.compressed_size) {
    Invalid("Stored ZIP member has inconsistent sizes");
  }
  return entry_info;
}

// Освобождает активное состояние zlib и помечает декодер остановленным.
void Reader::Stop() noexcept {
  if (_started) {
    inflateEnd(&_stream);
    _started = false;
  }
}

// Создаёт raw DEFLATE-декодер и сбрасывает состояние прохода по файлу.
void Reader::Start() {
  Stop();
  _input.resize(kChunk);
  _discard.resize(kChunk);
  _stream = {};
  const int status = inflateInit2(&_stream, -MAX_WBITS);
  if (status != Z_OK) {
    throw Error{ErrorKind::Memory, "ZIP decoder allocation failed"};
  }
  _started = true;
  _decoded = 0;
  _loaded = 0;
  _crc = 0;
  _ended = false;
  _verified = false;
}

// Проверяет состояние reader, отмену и неизменность внешнего архива.
void Reader::CheckState() {
  if (_failed) {
    Invalid("ZIP reader is in failed state; reopen the file");
  }
  _source.Check();
}

// Декодирует очередную порцию и проверяет размер, прогресс и конец потока.
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
    throw Error{ErrorKind::Memory, "ZIP decoder allocation failed"};
  }
  if (status != Z_OK && status != Z_STREAM_END) {
    Invalid("Invalid or truncated ZIP DEFLATE stream");
  }
  const size_t produced = count - _stream.avail_out;
  if (produced > _size - _decoded) {
    Invalid("ZIP decompressed size exceeds declared size");
  }
  _decoded += produced;
  _crc = crc32(_crc, reinterpret_cast<const Bytef*>(output.data()), produced);
  _ended = status == Z_STREAM_END;
  if (!_ended && !produced && input_before == _stream.avail_in) {
    Invalid("ZIP DEFLATE stream made no progress");
  }
  if (_ended &&
      (_decoded != _size || _loaded - _stream.avail_in != _compressed_size ||
       _crc != _expected_crc)) {
    Invalid("ZIP stream size or CRC mismatch");
  }
  _verified = _ended;
  return produced;
}

// Доводит декодер до нужной позиции, отбрасывая данные фиксированными порциями.
void Reader::Advance(uint64_t target) {
  if (!_started || target < _decoded) {
    Start();
  }
  while (_decoded < target) {
    const auto count = std::min<uint64_t>(_discard.size(), target - _decoded);
    Inflate(std::span{_discard}.first(count));
    if (_ended && _decoded < target) {
      Invalid("Unexpected ZIP EOF");
    }
  }
}

// Проверяет завершение полного прохода, размеры потока и CRC.
void Reader::Finish() {
  if (_method == 8 && !_verified) {
    if (!_started) {
      Start();
    }
    std::array<std::byte, 1> probe;
    while (!_ended) {
      Inflate(probe);
    }
  } else if (_method == 0 && _crc_valid && _crc_position == _size &&
             _crc != _expected_crc) {
    Invalid("ZIP CRC mismatch");
  }
}

// Читает Store или DEFLATE в буфер вызывающего кода и продвигает позицию.
size_t Reader::Read(std::span<std::byte> output) {
  try {
    CheckState();
    if (output.empty()) {
      return 0;
    }
    size_t count = std::min<uint64_t>(output.size(), _size - _position);
    if (_method == 8) {
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

// Устанавливает логическую позицию, откладывая распаковку до чтения.
void Reader::Seek(uint64_t offset) {
  CheckState();
  Range(offset, 0, _size);
  _position = offset;
  if (!offset && _method == 0) {
    _crc_position = 0;
    _crc_valid = true;
    _crc = 0;
  }
}

// Читает ровно заданный диапазон через установку позиции и обычное чтение.
void Reader::ReadAt(uint64_t offset, std::span<std::byte> output) {
  Range(offset, output.size(), _size);
  Seek(offset);
  Read(output);
}

}  // namespace sdb::connector::zip
