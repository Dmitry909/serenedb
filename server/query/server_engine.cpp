#include "query/server_engine.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include <duckdb.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/default/default_views.hpp>

#include "catalog/boot.h"
#include "connector/duckdb_copy_filesystem.h"
#include "connector/duckdb_foreign_server_function.h"
#include "connector/duckdb_pg_binary_copy.h"
#include "connector/duckdb_pg_text_copy.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_reindex_function.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_tokenizer_function.h"
#include "connector/duckdb_vacuum_function.h"
#include "connector/duckdb_zip_filesystem.hpp"
#include "connector/functions/ai/ai.h"
#include "connector/functions/array.h"
#include "connector/functions/catalog_introspect.h"
#include "connector/functions/duckdb_aliases.h"
#include "connector/functions/encode_key.h"
#include "connector/functions/es.h"
#include "connector/functions/inout.h"
#include "connector/functions/json.h"
#include "connector/functions/markdown_render.h"
#include "connector/functions/math.h"
#include "connector/functions/otel.h"
#include "connector/functions/ruleutils.h"
#include "connector/functions/search.h"
#include "connector/functions/string.h"
#include "connector/functions/system.h"
#include "connector/inverted_store_index.h"
#include "connector/iresearch_replacement_scan.h"
#include "connector/pg_logical_types.h"
#include "connector/scan/scan_function.h"
#include "connector/system_table_scan.h"
#include "docs/docs_functions.h"
#include "pg/commands/rbac.h"
#include "pg/pg_catalog/pg_statistic.h"
#include "pg/system_catalog.h"
#include "pg/system_table.h"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "server/utils/file_utils.h"
#include "server/utils/lifecycle.h"
#include "server/utils/number_of_cores.h"

extern "C" const duckdb::DefaultType* duckdb_external_types(
  duckdb::idx_t* count) {
  static const duckdb::DefaultType kExternalTypes[] = {
    {
      "regclass",
      sdb::pg::REGCLASS(),
      nullptr,
    },
    {
      "regtype",
      sdb::pg::REGTYPE(),
      nullptr,
    },
    {
      "regnamespace",
      sdb::pg::REGNAMESPACE(),
      nullptr,
    },
    {
      "regproc",
      sdb::pg::REGPROC(),
      nullptr,
    },
    {
      "regoper",
      sdb::pg::REGOPER(),
      nullptr,
    },
    {
      "regoperator",
      sdb::pg::REGOPERATOR(),
      nullptr,
    },
    {
      "regprocedure",
      sdb::pg::REGPROCEDURE(),
      nullptr,
    },
    {
      "regrole",
      sdb::pg::REGROLE(),
      nullptr,
    },
    {
      "regconfig",
      sdb::pg::REGCONFIG(),
      nullptr,
    },
    {
      "regdictionary",
      sdb::pg::REGDICTIONARY(),
      nullptr,
    },
    {
      "regcollation",
      sdb::pg::REGCOLLATION(),
      nullptr,
    },
    {
      "oid",
      sdb::pg::OID(),
      nullptr,
    },
    {
      "tid",
      sdb::pg::TID(),
      nullptr,
    },
    {
      "cid",
      sdb::pg::CID(),
      nullptr,
    },
    {
      "xid",
      sdb::pg::XID(),
      nullptr,
    },
    {
      "xid8",
      sdb::pg::XID8(),
      nullptr,
    },
    {
      "name",
      sdb::pg::NAME(),
      nullptr,
    },
    {
      "pg_statistic",
      sdb::pg::SystemTable<sdb::pg::PgStatistic>{}.RowType().WithAlias(
        "pg_statistic"),
      nullptr,
    },
    {
      "cardinal_number",
      sdb::pg::CARDINALNUMBER(),
      nullptr,
    },
    {
      "character_data",
      sdb::pg::CHARACTERDATA(),
      nullptr,
    },
    {
      "sql_identifier",
      sdb::pg::SQLIDENTIFIER(),
      nullptr,
    },
    {
      "time_stamp",
      sdb::pg::TIMESTAMP(),
      nullptr,
    },
    {
      "yes_or_no",
      sdb::pg::YESORNO(),
      nullptr,
    },
    {
      "serial",
      sdb::pg::SERIAL(),
      nullptr,
    },
    {
      "bigserial",
      sdb::pg::BIGSERIAL(),
      nullptr,
    },
    {
      "smallserial",
      sdb::pg::SMALLSERIAL(),
      nullptr,
    },
    {
      "void",
      sdb::pg::VOID(),
      nullptr,
    },
  };
  *count = std::size(kExternalTypes);
  return kExternalTypes;
}

ABSL_FLAG(uint64_t, cpu_threads, 0,
          "Executor pool size at process start. 0 = let server "
          "auto-detect from cpu_count. `SET GLOBAL threads = N` resizes the "
          "pool at runtime.");

ABSL_FLAG(uint32_t, recovery_replay_depth, 0,
          "Maximum WAL chunks in flight per inverted index during recovery "
          "replay (the prefetch window; bounds replay memory). 0 = auto "
          "(4 x cpu threads).");

ABSL_FLAG(bool, skip_search_recovery, false,
          "Do not replay the search-table WAL at startup; search tables come "
          "up with what their last refresh made durable.");

ABSL_DECLARE_FLAG(std::string, server_directory);

namespace sdb::server::query {

void ConfigureServerDBConfig(duckdb::DBConfig& config) {
  const auto datadir =
    lifecycle::ResolveDataDir(absl::GetFlag(FLAGS_server_directory));
  auto layout = duckdb::make_shared_ptr<catalog::DataDirectory>(datadir);
  connector::RegisterSereneDBStorage(config, layout);
  catalog::RegisterClusterStorage(config, std::move(layout));
  connector::RegisterConfigVariables(config);
  connector::RegisterIResearchReplacementScan(config);
  config.SetOptionByName(
    "temp_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "tmp")});
  config.SetOptionByName(
    "secret_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "secrets")});
  config.SetOptionByName(
    "extension_directory",
    duckdb::Value{utils::file_utils::BuildFilename(datadir, "extensions")});
  config.SetOptionByName("enable_view_dependencies",
                         duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("enable_macro_dependencies",
                         duckdb::Value::BOOLEAN(true));
  auto threads = absl::GetFlag(FLAGS_cpu_threads);
  if (threads == 0) {
    threads = CountLogicalCores();
    absl::SetFlag(&FLAGS_cpu_threads, threads);
  }
  config.SetOptionByName("threads", duckdb::Value::UBIGINT(threads));
  if (const auto depth = absl::GetFlag(FLAGS_recovery_replay_depth);
      depth != 0) {
    config.SetOptionByName(duckdb::Identifier{kRecoveryReplayDepthSetting},
                           duckdb::Value::UINTEGER(depth));
  }
  config.SetOptionByName("external_threads", duckdb::Value::UBIGINT(0));
  config.SetOptionByName("scheduler_process_partial",
                         duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("copy_csv_header_default",
                         duckdb::Value::BOOLEAN(false));
  config.SetOptionByName("integer_division", duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("show_behavior", duckdb::Value("SETTING"));
  config.SetOptionByName("autoinstall_known_extensions",
                         duckdb::Value::BOOLEAN(false));
  config.SetOptionByName("autoload_known_extensions",
                         duckdb::Value::BOOLEAN(false));
}

void RegisterServerExtensions(duckdb::DatabaseInstance& db) {
  connector::RegisterTokenizerPragma(db);

  connector::RegisterForeignServerPragma(db);

  connector::RegisterPgMathFunctions(db);

  connector::RegisterKeyEncodingFunctions(db);

  connector::RegisterPgSystemFunctions(db);

  connector::RegisterRuleutilsFunctions(db);

  connector::RegisterPgInOutFunctions(db);

  connector::RegisterPgStringFunctions(db);

  connector::RegisterPgArrayFunctions(db);

  connector::RegisterPgJsonFunctions(db);

  connector::RegisterEsFunctions(db);

  connector::RegisterOtelFunctions(db);

  connector::RegisterCatalogIntrospectFunctions(db);

  connector::RegisterMarkdownRenderFunctions(db);

  docs::RegisterDocsFunctions(db);

  connector::RegisterDuckDBAliases(db);

  connector::RegisterVacuumFunction(db);

  connector::RegisterReindexFunction(db);

  connector::RegisterPgBinaryCopyFunction(db);

  connector::RegisterPgTextCopyFunction(db);

  connector::RegisterSearchFunctions(db);

  connector::RegisterIResearchScanFunction(db);

  connector::RegisterSystemTableScanFunction(db);

  pg::RegisterRbacFunctions(db);

  connector::RegisterAIFunctions(db);

  connector::RegisterSereneDBOptimizers(db);

  auto& index_types = db.config.GetIndexTypes();
  index_types.RegisterIndexType(
    connector::InvertedStoreIndex::GetInvertedIndexType());

  auto& fs = duckdb::FileSystem::GetFileSystem(db);
  fs.RegisterSubSystem(duckdb::make_uniq<connector::SereneDBCopyFileSystem>());
  fs.RegisterSubSystem(duckdb::make_uniq<connector::ZipFileSystem>());

  auto parser = duckdb::Parser::GetBuiltinParser();
  pg::InitSystemFunctions(parser);
  pg::InitSystemViews(parser);
}

}
