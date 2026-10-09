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

#include "connector/duckdb_zip_filesystem.hpp"

#include <sys/stat.h>

#include <cerrno>
#include <cstring>
#include <duckdb/common/file_opener.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <mutex>
#include <new>
#include <optional>

namespace sdb::connector {
namespace {

// Преобразует ошибку ZIP-reader в соответствующее исключение DuckDB.
[[noreturn]] void Translate(const zip::Error& error) {
  if (error.kind == zip::ErrorKind::Memory) {
    throw duckdb::OutOfMemoryException("%s", error.what());
  }
  if (error.kind == zip::ErrorKind::Unsupported) {
    throw duckdb::NotImplementedException("%s", error.what());
  }
  throw duckdb::IOException("%s", error.what());
}

class LocalSource final : public zip::Source {
 public:
  // Принимает локальный файл и сохраняет его размер, время изменения и
  // контекст.
  LocalSource(duckdb::unique_ptr<duckdb::FileSystem> fs,
              duckdb::unique_ptr<duckdb::FileHandle> file,
              duckdb::optional_ptr<duckdb::ClientContext> client)
    : fs{std::move(fs)},
      file{std::move(file)},
      size{this->file->GetFileSize()},
      modified{this->fs->GetLastModifiedTime(*this->file)} {
    if (client) {
      context = client->shared_from_this();
    }
  }
  // Возвращает размер архива, зафиксированный при открытии.
  uint64_t Size() const final { return size; }
  // Проверяет отмену запроса и изменение размера или времени модификации
  // архива.
  void Check() final {
    if (auto client = context.lock()) {
      client->InterruptCheck();
    }
    if (file->GetFileSize() != size ||
        fs->GetLastModifiedTime(*file) != modified) {
      throw duckdb::IOException("ZIP archive changed while reading");
    }
  }
  // Читает точный диапазон локального архива после проверки границ и состояния.
  void ReadAt(uint64_t offset, std::span<std::byte> output) final {
    Check();
    if (offset > uint64_t(size) || output.size() > uint64_t(size) - offset) {
      throw duckdb::IOException("ZIP read exceeds archive bounds");
    }
    file->Read(output.data(), output.size(), offset);
  }

  duckdb::unique_ptr<duckdb::FileSystem> fs;
  duckdb::unique_ptr<duckdb::FileHandle> file;
  uint64_t size;
  duckdb::timestamp_t modified;
  duckdb::weak_ptr<duckdb::ClientContext> context;
};

class ZipHandle final : public duckdb::FileHandle {
 public:
  // Создаёт handle файла ZIP с независимым источником и reader.
  ZipHandle(duckdb::FileSystem& fs, const std::string& path,
            duckdb::FileOpenFlags flags,
            duckdb::unique_ptr<duckdb::FileSystem> local,
            duckdb::unique_ptr<duckdb::FileHandle> file,
            duckdb::optional_ptr<duckdb::ClientContext> client,
            std::string_view member)
    : FileHandle{fs, path, flags},
      source{std::move(local), std::move(file), client},
      reader{std::in_place, source, zip::FindEntry(source, member)} {}
  // Закрывает reader и внешний файл при уничтожении handle.
  ~ZipHandle() noexcept final { Close(); }
  // Идемпотентно освобождает reader и внешний файл под блокировкой handle.
  void Close() final {
    std::lock_guard lock{mutex};
    reader.reset();
    source.file.reset();
  }
  // Возвращает reader либо сообщает, что handle уже закрыт.
  zip::Reader& GetReader() {
    if (!reader) {
      throw duckdb::IOException("ZIP file is closed");
    }
    return *reader;
  }
  LocalSource source;
  std::optional<zip::Reader> reader;
  std::mutex mutex;
};

// Отклоняет отрицательную длину чтения.
void ValidateCount(int64_t count) {
  if (count < 0) {
    throw duckdb::IOException("Negative ZIP read length");
  }
}

}  // namespace

// Определяет принадлежность пути файловой системе ZIP по префиксу.
bool ZipFileSystem::CanHandleFile(const std::string& path) {
  return path.starts_with("zip://");
}

// Проверяет права и локальность источника, затем открывает выбранный файл ZIP.
duckdb::unique_ptr<duckdb::FileHandle> ZipFileSystem::OpenFile(
  const std::string& path, duckdb::FileOpenFlags flags,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  try {
    const auto parsed = zip::ParsePath(path);
    if (!flags.OpenForReading() || flags.OpenForWriting() ||
        flags.CreateFileIfNotExists() || flags.OverwriteExistingFile() ||
        flags.OpenForAppending()) {
      throw duckdb::NotImplementedException("ZIP filesystem is read-only");
    }
    auto db = duckdb::FileOpener::TryGetDatabase(opener);
    if (!db) {
      throw duckdb::IOException("ZIP reading requires a database file opener");
    }
    auto local = duckdb::FileSystem::CreateLocal();
    const auto archive = local->ExpandPath(std::string{parsed.archive}, opener);
    auto& registered = duckdb::FileSystem::GetFileSystem(*db);
    if (registered.SubSystemIsDisabled(local->GetName())) {
      throw duckdb::PermissionException("Local filesystem is disabled");
    }
    if (!db->config.CanAccessFile(archive,
                                  duckdb::FileType::FILE_TYPE_REGULAR)) {
      throw duckdb::PermissionException("Cannot access ZIP archive: %s",
                                        archive);
    }
    // Проверяет тип до открытия: чтение FIFO может заблокировать поток.
    struct stat stat;
    if (::stat(archive.c_str(), &stat) != 0) {
      if (errno == ENOENT && flags.ReturnNullIfNotExists()) {
        return nullptr;
      }
      throw duckdb::IOException("Cannot stat ZIP archive '%s': %s", archive,
                                std::strerror(errno));
    }
    if (!S_ISREG(stat.st_mode)) {
      throw duckdb::NotImplementedException(
        "ZIP source must be a regular local file");
    }
    const auto canonical = local->CanonicalizePath(archive, opener);
    if (!db->config.CanAccessFile(canonical,
                                  duckdb::FileType::FILE_TYPE_REGULAR)) {
      throw duckdb::PermissionException("Cannot access ZIP archive: %s",
                                        canonical);
    }
    auto file =
      local->OpenFile(archive, duckdb::FileFlags::FILE_FLAGS_READ, opener);
    if (file->GetType() != duckdb::FileType::FILE_TYPE_REGULAR ||
        !file->CanSeek()) {
      throw duckdb::IOException("ZIP source must remain a regular local file");
    }
    return duckdb::make_uniq<ZipHandle>(
      *this, path, flags, std::move(local), std::move(file),
      duckdb::FileOpener::TryGetClientContext(opener), parsed.member);
  } catch (const zip::Error& error) {
    if (error.kind == zip::ErrorKind::Missing &&
        flags.ReturnNullIfNotExists()) {
      return nullptr;
    }
    Translate(error);
  } catch (const std::bad_alloc&) {
    throw duckdb::OutOfMemoryException("ZIP allocation failed");
  }
}

// Выполняет последовательное чтение с блокировкой handle и переводом ошибок.
int64_t ZipFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                            int64_t count) {
  ValidateCount(count);
  auto& zip = handle.Cast<ZipHandle>();
  std::lock_guard lock{zip.mutex};
  try {
    return zip.GetReader().Read(
      {static_cast<std::byte*>(buffer), static_cast<size_t>(count)});
  } catch (const zip::Error& error) {
    Translate(error);
  } catch (const std::bad_alloc&) {
    throw duckdb::OutOfMemoryException("ZIP allocation failed");
  }
}

// Читает точный диапазон файла с заданного смещения под блокировкой handle.
void ZipFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                         int64_t count, duckdb::idx_t offset) {
  ValidateCount(count);
  auto& zip = handle.Cast<ZipHandle>();
  std::lock_guard lock{zip.mutex};
  try {
    zip.GetReader().ReadAt(
      offset, {static_cast<std::byte*>(buffer), static_cast<size_t>(count)});
  } catch (const zip::Error& error) {
    Translate(error);
  } catch (const std::bad_alloc&) {
    throw duckdb::OutOfMemoryException("ZIP allocation failed");
  }
}

// Устанавливает логическую позицию reader и переводит ошибки в DuckDB.
void ZipFileSystem::Seek(duckdb::FileHandle& handle, duckdb::idx_t offset) {
  auto& zip = handle.Cast<ZipHandle>();
  std::lock_guard lock{zip.mutex};
  try {
    zip.GetReader().Seek(offset);
  } catch (const zip::Error& error) {
    Translate(error);
  } catch (const std::bad_alloc&) {
    throw duckdb::OutOfMemoryException("ZIP allocation failed");
  }
}

// Возвращает логическую позицию файла к началу.
void ZipFileSystem::Reset(duckdb::FileHandle& handle) { Seek(handle, 0); }

// Возвращает текущую логическую позицию в распакованном файле.
duckdb::idx_t ZipFileSystem::SeekPosition(duckdb::FileHandle& handle) {
  auto& zip = handle.Cast<ZipHandle>();
  std::lock_guard lock{zip.mutex};
  return zip.GetReader().Position();
}

// Возвращает проверенный распакованный размер выбранного файла.
int64_t ZipFileSystem::GetFileSize(duckdb::FileHandle& handle) {
  auto& zip = handle.Cast<ZipHandle>();
  std::lock_guard lock{zip.mutex};
  return zip.GetReader().Size();
}

// Возвращает время изменения внешнего архива для открытого файла.
duckdb::timestamp_t ZipFileSystem::GetLastModifiedTime(
  duckdb::FileHandle& handle) {
  auto& zip = handle.Cast<ZipHandle>();
  std::lock_guard lock{zip.mutex};
  zip.GetReader();
  return zip.source.modified;
}

// Представляет выбранный файл архива как обычный файл для DuckDB.
duckdb::FileType ZipFileSystem::GetFileType(duckdb::FileHandle& handle) {
  return duckdb::FileType::FILE_TYPE_REGULAR;
}

// Проверяет наличие файла, сохраняя ошибки доступа и повреждения архива.
bool ZipFileSystem::FileExists(
  const std::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) {
  return bool(OpenFile(path,
                       duckdb::FileFlags::FILE_FLAGS_READ |
                         duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS,
                       opener));
}

// Возвращает один проверенный точный путь; шаблоны отклоняются парсером.
duckdb::vector<duckdb::OpenFileInfo> ZipFileSystem::Glob(
  const std::string& path, duckdb::FileOpener* opener) {
  try {
    zip::ParsePath(path);
  } catch (const zip::Error& error) {
    Translate(error);
  } catch (const std::bad_alloc&) {
    throw duckdb::OutOfMemoryException("ZIP allocation failed");
  }
  return {duckdb::OpenFileInfo{path}};
}

// Сообщает, что перечисление содержимого ZIP пока не поддерживается.
bool ZipFileSystem::ListFiles(
  const std::string& path,
  const std::function<void(const std::string&, bool)>& callback,
  duckdb::FileOpener* opener) {
  throw duckdb::NotImplementedException(
    "ZIP directory listing is not supported yet");
}

}  // namespace sdb::connector
