#pragma once

#include <duckdb/common/file_system.hpp>

#include "connector/zip_reader.hpp"

namespace sdb::connector {

class ZipFileSystem final : public duckdb::FileSystem {
 public:
  std::string GetName() const final { return "ZipFileSystem"; }
  bool CanHandleFile(const std::string& path) final;
  bool CanSeek() final { return true; }
  bool OnDiskFile(duckdb::FileHandle& handle) final { return true; }
  bool IsPipe(const std::string& path,
              duckdb::optional_ptr<duckdb::FileOpener> opener) final {
    return false;
  }

  duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
    const std::string& path, duckdb::FileOpenFlags flags,
    duckdb::optional_ptr<duckdb::FileOpener> opener) final;
  int64_t Read(duckdb::FileHandle& handle, void* buffer, int64_t count) final;
  void Read(duckdb::FileHandle& handle, void* buffer, int64_t count,
            duckdb::idx_t offset) final;
  void Seek(duckdb::FileHandle& handle, duckdb::idx_t offset) final;
  void Reset(duckdb::FileHandle& handle) final;
  duckdb::idx_t SeekPosition(duckdb::FileHandle& handle) final;
  int64_t GetFileSize(duckdb::FileHandle& handle) final;
  duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle& handle) final;
  duckdb::FileType GetFileType(duckdb::FileHandle& handle) final;
  bool FileExists(const std::string& path,
                  duckdb::optional_ptr<duckdb::FileOpener> opener) final;
  duckdb::vector<duckdb::OpenFileInfo> Glob(const std::string& path,
                                            duckdb::FileOpener* opener) final;
  bool ListFiles(const std::string& path,
                 const std::function<void(const std::string&, bool)>& callback,
                 duckdb::FileOpener* opener) final;
};

}
