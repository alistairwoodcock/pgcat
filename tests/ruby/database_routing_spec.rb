# frozen_string_literal: true

require_relative 'spec_helper'

describe "Database Routing" do
  let(:processes) { Helpers::Pgcat.multi_pool_setup("transaction", "info") }

  after do
    processes.all_databases.each(&:reset)
    processes.pgcat.shutdown
  end

  describe "basic routing" do
    context "when database_regex is not configured" do
      it "ignores database routing comments and uses default pool" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
        10.times { conn.async_exec("/* database: analytics_db */ SELECT 1 + 2") }

        expect(processes.main_db.count_select_1_plus_2).to eq(10)
        expect(processes.analytics_db.count_select_1_plus_2).to eq(0)
      end
    end

    context "when database_regex is configured" do
      before do
        current_configs = processes.pgcat.current_config
        current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
        current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db", "reporting_db"]

        processes.pgcat.update_config(current_configs)
        processes.pgcat.reload_config
      end

      it "routes query to target database when comment matches" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
        10.times { conn.async_exec("/* database: analytics_db */ SELECT 1 + 2") }

        expect(processes.main_db.count_select_1_plus_2).to eq(0)
        expect(processes.analytics_db.count_select_1_plus_2).to eq(10)
      end

      it "uses default pool when no comment is present" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
        10.times { conn.async_exec("SELECT 1 + 2") }

        expect(processes.main_db.count_select_1_plus_2).to eq(10)
        expect(processes.analytics_db.count_select_1_plus_2).to eq(0)
      end

      it "clears routing state between queries" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

        # First query routed to analytics_db
        conn.async_exec("/* database: analytics_db */ SELECT 1 + 2")
        # Second query should go back to main_db (no comment)
        conn.async_exec("SELECT 1 + 2")

        expect(processes.main_db.count_select_1_plus_2).to eq(1)
        expect(processes.analytics_db.count_select_1_plus_2).to eq(1)
      end

      it "routes to different databases in sequence" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

        5.times { conn.async_exec("/* database: analytics_db */ SELECT 1 + 2") }
        5.times { conn.async_exec("/* database: reporting_db */ SELECT 1 + 2") }
        5.times { conn.async_exec("SELECT 1 + 2") }

        expect(processes.main_db.count_select_1_plus_2).to eq(5)
        expect(processes.analytics_db.count_select_1_plus_2).to eq(5)
        expect(processes.reporting_db.count_select_1_plus_2).to eq(5)
      end
    end
  end

  describe "allowlist enforcement" do
    before do
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
      current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db"]
      current_configs["pools"]["main_db"]["allow_all_databases"] = false

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end

    it "allows routing to databases in allowlist" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
      10.times { conn.async_exec("/* database: analytics_db */ SELECT 1 + 2") }

      expect(processes.analytics_db.count_select_1_plus_2).to eq(10)
    end

    it "rejects routing to databases not in allowlist" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

      expect {
        conn.async_exec("/* database: reporting_db */ SELECT 1 + 2")
      }.to raise_error(PG::Error, /not found or user.*not authorized/)

      expect(processes.reporting_db.count_select_1_plus_2).to eq(0)
    end

    context "when allow_all_databases is true" do
      before do
        current_configs = processes.pgcat.current_config
        current_configs["pools"]["main_db"]["allow_all_databases"] = true

        processes.pgcat.update_config(current_configs)
        processes.pgcat.reload_config
      end

      it "allows routing to any configured database" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

        5.times { conn.async_exec("/* database: analytics_db */ SELECT 1 + 2") }
        5.times { conn.async_exec("/* database: reporting_db */ SELECT 1 + 2") }

        expect(processes.analytics_db.count_select_1_plus_2).to eq(5)
        expect(processes.reporting_db.count_select_1_plus_2).to eq(5)
      end
    end
  end

  describe "user authorization" do
    let(:processes) { Helpers::Pgcat.multi_pool_setup_with_restricted_user("transaction", "info") }

    before do
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
      current_configs["pools"]["main_db"]["allow_all_databases"] = true

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end

    it "succeeds when user exists in both pools" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
      conn.async_exec("/* database: analytics_db */ SELECT 1 + 2")

      expect(processes.analytics_db.count_select_1_plus_2).to eq(1)
    end

    it "fails when user does not exist in target pool" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "other_user"))

      expect {
        conn.async_exec("/* database: analytics_db */ SELECT 1 + 2")
      }.to raise_error(PG::Error, /not found or user.*not authorized/)
    end
  end

  describe "extended protocol (prepared statements)" do
    before do
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
      current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db"]

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end

    it "routes prepared statements within a transaction" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

      # Prepared statements need to be in a transaction to ensure the same
      # connection is used for prepare and execute. The routing directive
      # must be on the first statement that acquires a connection.
      conn.exec("/* database: analytics_db */ BEGIN")
      conn.prepare("routed_stmt", "SELECT $1::int + $2::int")
      10.times { conn.exec_prepared("routed_stmt", [1, 2]) }
      conn.exec("COMMIT")

      expect(processes.analytics_db.count_query("SELECT $1::int + $2::int")).to be >= 10
    end

    it "routes parameterized queries with database comment" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

      # Each exec_params call sends a Parse message containing the query,
      # so the routing comment should be parsed for each call
      10.times do
        conn.exec_params("/* database: analytics_db */ SELECT $1::int + $2::int", [1, 2])
      end

      # The comment is preserved in the query stored by pg_stat_statements
      expect(processes.analytics_db.count_query("/* database: analytics_db */ SELECT $1::int + $2::int")).to be >= 10
    end
  end

  describe "transaction behavior" do
    before do
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
      current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db"]

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end

    context "in transaction mode" do
      it "routing is determined by first statement that acquires a connection" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

        # The routing directive MUST be on the first statement that acquires
        # a connection (typically BEGIN). Otherwise, the connection is acquired
        # from the default pool before routing can take effect.
        conn.exec("/* database: analytics_db */ BEGIN")
        conn.exec("SELECT 1 + 2")
        conn.exec("SELECT 1 + 2")
        conn.exec("COMMIT")

        # Both queries went to analytics_db because routing was set on BEGIN
        expect(processes.analytics_db.count_select_1_plus_2).to eq(2)
        expect(processes.main_db.count_select_1_plus_2).to eq(0)
      end

      it "clears routing state after transaction ends" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

        # First transaction to analytics_db
        conn.exec("/* database: analytics_db */ BEGIN")
        conn.exec("SELECT 1 + 2")
        conn.exec("COMMIT")

        # Second query without routing directive should go to main_db
        conn.exec("SELECT 1 + 2")

        expect(processes.analytics_db.count_select_1_plus_2).to eq(1)
        expect(processes.main_db.count_select_1_plus_2).to eq(1)
      end
    end

    context "in session mode" do
      let(:processes) { Helpers::Pgcat.multi_pool_setup("session", "info") }

      before do
        current_configs = processes.pgcat.current_config
        current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
        current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db"]

        processes.pgcat.update_config(current_configs)
        processes.pgcat.reload_config
      end

      it "first routed query determines server for entire session" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

        # First query routes to analytics_db
        conn.exec("/* database: analytics_db */ SELECT 1 + 2")
        # In session mode, subsequent queries go to the same server
        conn.exec("SELECT 1 + 2")
        conn.exec("SELECT 1 + 2")

        # All queries went to analytics_db
        expect(processes.analytics_db.count_select_1_plus_2).to eq(3)
        expect(processes.main_db.count_select_1_plus_2).to eq(0)
      end
    end
  end

  describe "config reload during queries" do
    before do
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
      current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db", "reporting_db"]

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end

    it "handles concurrent config reloads gracefully" do
      threads = []
      errors = []

      # Thread 1: Continuous queries with routing
      threads << Thread.new do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
        50.times do
          begin
            conn.exec("/* database: analytics_db */ SELECT pg_sleep(0.01)")
          rescue PG::Error => e
            errors << e
          end
        end
      end

      # Thread 2: Config reloads
      threads << Thread.new do
        5.times do
          begin
            processes.pgcat.reload_config
          rescue StandardError => e
            errors << e
          end
          sleep 0.05
        end
      end

      threads.each(&:join)

      # Should complete without fatal errors
      fatal_errors = errors.select { |e| e.message.include?("FATAL") }
      expect(fatal_errors).to be_empty
    end

    it "applies new allowlist after config reload" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

      # Initially reporting_db is allowed
      conn.exec("/* database: reporting_db */ SELECT 1 + 2")
      expect(processes.reporting_db.count_select_1_plus_2).to eq(1)

      # Remove reporting_db from allowlist
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db"]
      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config

      # Now routing to reporting_db should fail
      expect {
        conn.exec("/* database: reporting_db */ SELECT 1 + 2")
      }.to raise_error(PG::Error)
    end
  end

  describe "edge cases" do
    before do
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
      current_configs["pools"]["main_db"]["allow_all_databases"] = true

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end

    it "handles comment at the beginning of query" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
      conn.exec("/* database: analytics_db */ SELECT 1 + 2")

      expect(processes.analytics_db.count_select_1_plus_2).to eq(1)
    end

    it "handles comment after SELECT keyword" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
      conn.exec("SELECT /* database: analytics_db */ 1 + 2")

      expect(processes.analytics_db.count_select_1_plus_2).to eq(1)
    end

    it "falls back to default when routing to non-existent pool" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))

      expect {
        conn.exec("/* database: nonexistent_db */ SELECT 1 + 2")
      }.to raise_error(PG::Error, /not found/)
    end

    it "handles multiple database comments (first one wins)" do
      conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
      conn.exec("/* database: analytics_db */ /* database: reporting_db */ SELECT 1 + 2")

      # First match wins
      expect(processes.analytics_db.count_select_1_plus_2).to eq(1)
      expect(processes.reporting_db.count_select_1_plus_2).to eq(0)
    end

    context "with small regex_search_limit" do
      before do
        current_configs = processes.pgcat.current_config
        current_configs["pools"]["main_db"]["regex_search_limit"] = 50

        processes.pgcat.update_config(current_configs)
        processes.pgcat.reload_config
      end

      it "ignores comment beyond search limit" do
        conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
        # Padding to push comment beyond limit
        padding = "x" * 100
        conn.exec("SELECT '#{padding}' /* database: analytics_db */")

        # Comment not found (beyond regex_search_limit), goes to main_db
        # Check that queries went to main_db and not analytics_db
        main_count = processes.main_db.with_connection do |c|
          c.exec("SELECT COUNT(*) FROM pg_stat_statements WHERE query LIKE '%analytics_db%'")[0]["count"].to_i
        end
        analytics_count = processes.analytics_db.with_connection do |c|
          c.exec("SELECT COUNT(*) FROM pg_stat_statements WHERE query LIKE '%analytics_db%'")[0]["count"].to_i
        end
        expect(main_count).to be >= 1
        expect(analytics_count).to eq(0)
      end
    end
  end

  describe "concurrent routing" do
    before do
      current_configs = processes.pgcat.current_config
      current_configs["pools"]["main_db"]["database_regex"] = '/\* database: (\w+) \*/'
      current_configs["pools"]["main_db"]["allowed_databases"] = ["analytics_db", "reporting_db"]

      processes.pgcat.update_config(current_configs)
      processes.pgcat.reload_config
    end

    it "handles multiple clients routing to different databases" do
      threads = 5.times.map do |i|
        Thread.new do
          conn = PG.connect(processes.pgcat.connection_string("main_db", "sharding_user"))
          target = i.even? ? "analytics_db" : "reporting_db"
          20.times do
            conn.async_exec("/* database: #{target} */ SELECT 1 + 2")
          end
        end
      end

      threads.each(&:join)

      # 3 even threads (0, 2, 4) -> analytics_db: 60 queries
      # 2 odd threads (1, 3) -> reporting_db: 40 queries
      expect(processes.analytics_db.count_select_1_plus_2).to eq(60)
      expect(processes.reporting_db.count_select_1_plus_2).to eq(40)
      expect(processes.main_db.count_select_1_plus_2).to eq(0)
    end
  end
end
