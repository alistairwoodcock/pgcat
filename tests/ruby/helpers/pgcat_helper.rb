require 'json'
require 'ostruct'
require_relative 'pgcat_process'
require_relative 'pg_instance'
require_relative 'pg_socket'

class ::Hash
    def deep_merge(second)
        merger = proc { |key, v1, v2| Hash === v1 && Hash === v2 ? v1.merge(v2, &merger) : v2 }
        self.merge(second, &merger)
    end
end

module Helpers
  module Pgcat
    def self.three_shard_setup(pool_name, pool_size, pool_mode="transaction", lb_mode="random", log_level="info")
      user = {
        "password" => "sharding_user",
        "pool_size" => pool_size,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      pgcat    = PgcatProcess.new(log_level)
      primary0 = PgInstance.new(5432, user["username"], user["password"], "shard0")
      primary1 = PgInstance.new(7432, user["username"], user["password"], "shard1")
      primary2 = PgInstance.new(8432, user["username"], user["password"], "shard2")

      pgcat_cfg = pgcat.current_config
      pgcat_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "any",
          "pool_mode" => pool_mode,
          "load_balancing_mode" => lb_mode,
          "primary_reads_enabled" => true,
          "query_parser_enabled" => true,
          "query_parser_read_write_splitting" => true,
          "automatic_sharding_key" => "data.id",
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => { "database" => "shard0", "servers" => [["localhost", primary0.port.to_i, "primary"]] },
            "1" => { "database" => "shard1", "servers" => [["localhost", primary1.port.to_i, "primary"]] },
            "2" => { "database" => "shard2", "servers" => [["localhost", primary2.port.to_i, "primary"]] },
          },
          "users" => { "0" => user },
          "plugins" => {
            "intercept" => {
              "enabled" => true,
              "queries" => {
                "0" => {
                  "query" => "select current_database() as a, current_schemas(false) as b",
                  "schema" => [
                      ["a", "text"],
                      ["b", "text"],
                  ],
                  "result" => [
                    ["${DATABASE}", "{public}"],
                  ]
                }
              }
            }
          }
        }
      }
      pgcat.update_config(pgcat_cfg)

      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.shards = [primary0, primary1, primary2]
        struct.all_databases = [primary0, primary1, primary2]
      end
    end

    def self.single_instance_setup(pool_name, pool_size, pool_mode="transaction", lb_mode="random", log_level="trace")
      user = {
        "password" => "sharding_user",
        "pool_size" => pool_size,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      pgcat = PgcatProcess.new(log_level)
      pgcat_cfg = pgcat.current_config

      primary  = PgInstance.new(5432, user["username"], user["password"], "shard0")

      # Main proxy configs
      pgcat_cfg["pools"] = {
        "#{pool_name}" => {
          "default_role" => "primary",
          "pool_mode" => pool_mode,
          "load_balancing_mode" => lb_mode,
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard0",
              "servers" => [
                ["localhost", primary.port.to_i, "primary"]
              ]
            },
          },
          "users" => { "0" => user }
        }
      }
      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg)
      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.primary = primary
        struct.all_databases = [primary]
      end
    end

    def self.single_shard_setup(pool_name, pool_size, pool_mode="transaction", lb_mode="random", log_level="info", pool_settings={})
      user = {
        "password" => "sharding_user",
        "pool_size" => pool_size,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      pgcat = PgcatProcess.new(log_level)
      pgcat_cfg = pgcat.current_config

      primary  = PgInstance.new(5432, user["username"], user["password"], "shard0")
      replica0 = PgInstance.new(7432, user["username"], user["password"], "shard0")
      replica1 = PgInstance.new(8432, user["username"], user["password"], "shard0")
      replica2 = PgInstance.new(9432, user["username"], user["password"], "shard0")

      pool_config = {
        "default_role" => "any",
        "pool_mode" => pool_mode,
        "load_balancing_mode" => lb_mode,
        "primary_reads_enabled" => false,
        "query_parser_enabled" => false,
        "sharding_function" => "pg_bigint_hash",
        "shards" => {
          "0" => {
            "database" => "shard0",
            "servers" => [
              ["localhost", primary.port.to_i, "primary"],
              ["localhost", replica0.port.to_i, "replica"],
              ["localhost", replica1.port.to_i, "replica"],
              ["localhost", replica2.port.to_i, "replica"]
            ]
          },
        },
        "users" => { "0" => user }
      }

      pool_config = pool_config.merge(pool_settings)

      # Main proxy configs
      pgcat_cfg["pools"] = {
        "#{pool_name}" => pool_config,
      }
      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg)
      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.primary = primary
        struct.replicas = [replica0, replica1, replica2]
        struct.all_databases = [primary, replica0, replica1, replica2]
      end
    end

    # Setup for database routing tests - two separate pools with the same user
    # This allows testing routing queries from one pool to another
    def self.multi_pool_setup(pool_mode="transaction", log_level="info")
      user = {
        "password" => "sharding_user",
        "pool_size" => 5,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      pgcat = PgcatProcess.new(log_level)
      pgcat_cfg = pgcat.current_config

      # Two separate database instances representing different pools
      main_db = PgInstance.new(5432, user["username"], user["password"], "shard0")
      analytics_db = PgInstance.new(7432, user["username"], user["password"], "shard1")
      reporting_db = PgInstance.new(8432, user["username"], user["password"], "shard2")

      # Configure three pools - main_db, analytics_db, and reporting_db
      pgcat_cfg["pools"] = {
        "main_db" => {
          "default_role" => "primary",
          "pool_mode" => pool_mode,
          "load_balancing_mode" => "random",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard0",
              "servers" => [["localhost", main_db.port.to_i, "primary"]]
            }
          },
          "users" => { "0" => user }
        },
        "analytics_db" => {
          "default_role" => "primary",
          "pool_mode" => pool_mode,
          "load_balancing_mode" => "random",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard1",
              "servers" => [["localhost", analytics_db.port.to_i, "primary"]]
            }
          },
          "users" => { "0" => user }
        },
        "reporting_db" => {
          "default_role" => "primary",
          "pool_mode" => pool_mode,
          "load_balancing_mode" => "random",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard2",
              "servers" => [["localhost", reporting_db.port.to_i, "primary"]]
            }
          },
          "users" => { "0" => user }
        }
      }

      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg)
      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.main_db = main_db
        struct.analytics_db = analytics_db
        struct.reporting_db = reporting_db
        struct.all_databases = [main_db, analytics_db, reporting_db]
      end
    end

    # Setup for database routing with a user that only exists in main_db
    # Uses sharding_user (exists in both) and other_user (configured only in main_db pool)
    def self.multi_pool_setup_with_restricted_user(pool_mode="transaction", log_level="info")
      shared_user = {
        "password" => "sharding_user",
        "pool_size" => 5,
        "statement_timeout" => 0,
        "username" => "sharding_user"
      }

      # This user exists in postgres but we only configure it in main_db pool
      main_only_user = {
        "password" => "other_user",
        "pool_size" => 5,
        "statement_timeout" => 0,
        "username" => "other_user"
      }

      pgcat = PgcatProcess.new(log_level)
      pgcat_cfg = pgcat.current_config

      main_db = PgInstance.new(5432, shared_user["username"], shared_user["password"], "shard0")
      analytics_db = PgInstance.new(7432, shared_user["username"], shared_user["password"], "shard1")

      pgcat_cfg["pools"] = {
        "main_db" => {
          "default_role" => "primary",
          "pool_mode" => pool_mode,
          "load_balancing_mode" => "random",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard0",
              "servers" => [["localhost", main_db.port.to_i, "primary"]]
            }
          },
          "users" => {
            "0" => shared_user,
            "1" => main_only_user  # This user only configured in main_db pool
          }
        },
        "analytics_db" => {
          "default_role" => "primary",
          "pool_mode" => pool_mode,
          "load_balancing_mode" => "random",
          "primary_reads_enabled" => false,
          "query_parser_enabled" => false,
          "sharding_function" => "pg_bigint_hash",
          "shards" => {
            "0" => {
              "database" => "shard1",
              "servers" => [["localhost", analytics_db.port.to_i, "primary"]]
            }
          },
          "users" => { "0" => shared_user }  # other_user not configured here
        }
      }

      pgcat_cfg["general"]["port"] = pgcat.port
      pgcat.update_config(pgcat_cfg)
      pgcat.start
      pgcat.wait_until_ready

      OpenStruct.new.tap do |struct|
        struct.pgcat = pgcat
        struct.main_db = main_db
        struct.analytics_db = analytics_db
        struct.all_databases = [main_db, analytics_db]
      end
    end
  end
end
