require 'tempfile'
require 'action_view'
require 'capistrano'

module CapistranoSyncTask
  module Helper
    def get_ssh_command()
      server = cap.find_servers.first
      srv = server.host
      srv += ":#{server.port}" if server.port
      ["ssh -l #{cap[:user]} #{srv}", srv]
    end

    def log(*args)
      cap.logger.info *args
    end

    # missing capture
    def _capture(command, options={})
      output = ""
      cap.invoke_command(command, options.merge(:once => true)) do |ch, stream, data|
        case stream
        when :out then output << data
        when :err then warn "[err :: #{ch[:server]}] #{data}"
        end
      end
      output
    end
    
    def check_deps
      system "which pv 2> /dev/null"
      unless ($?.to_i == 0)
        puts "FATAL: pv (Pipe Viewer) command not found, please install 'port install pv' or 'brew install pv'"
        exit(1)
      end
    end
  end

  class Db
    include Helper
    attr_accessor :rails_env, :local_rails_env, :tables, :cap

    def initialize(cap, rails_env, local_rails_env, tables)
      self.rails_env = rails_env
      self.local_rails_env = local_rails_env
      self.tables = tables
      self.cap = cap
    end

    def sync
      check_deps
      remote_config = remote_database_config
      local_config  = local_database_config

      dump_method = remote_config[:adapter] + "_dump"
      load_method = local_config[:adapter] + "_load"
      unless self.private_methods.include?(dump_method)
        puts "FATAL: Can't dump: unknown adapter #{remote_config[:adapter].inspect}"
        exit(1)
      end
      unless self.private_methods.include?(load_method)
        puts "FATAL: Can't load: unknown adapter #{local_config[:adapter].inspect}"
        exit(1)
      end

      dump_command    = __send__(dump_method, remote_config)
      load_command    = __send__(load_method, local_config)
      ssh_cmd, server = get_ssh_command

      log "drop and create local database"
      drop_and_create_local_db
      log "dump from #{server} and load to local #{local_rails_env} db (see progress)"

      cmd = "#{ssh_cmd} #{dump_command} | pv | #{load_command}"
      system cmd
    end

    private
    def drop_and_create_local_db
      system "bundle exec rake -q db:drop db:create RAILS_ENV=#{local_rails_env} 2&>1 /dev/null"
    end

    def remote_database_config
      unless @remove_database_config
        remote_config = _capture "cat #{cap.current_path}/config/database.yml"
        @remove_database_config = load_database_config(remote_config, rails_env)
      end
      @remove_database_config
    end

    def local_database_config
      unless @local_database_config
        local_config = IO.read("config/database.yml")
        @local_database_config = load_database_config(local_config, self.local_rails_env)
      end
      @local_database_config
    end

    def load_database_config(io, env)
      config = YAML.load(io)[env]
      raise "Can't read #{env} entry from database.yml" unless config
      {
        :adapter  => config["adapter"],
        :user     => config["username"] || config["user"],
        :pass     => config["password"],
        :dbname   => config["database"],
        :socket   => config["socket"],
        :host     => config["host"],
      }
    end

    def postgresql_dump(config)
      dump_cmd = "pg_dump"
      dump_cmd << " --no-owner --no-privileges --disable-triggers --inserts"
      dump_cmd << " --username=#{config[:user]}" if config[:user]
      #dump_cmd << " --password=#{config[:pass]}" if config[:pass]
      dump_cmd << " --host=#{config[:host]}" if config[:host]
      dump_cmd << " #{config[:dbname]}"
    end

    def postgresql_load(config)
      load_cmd = "psql --single-transaction --quiet -o /dev/null"
      load_cmd << " --username=#{config[:user]}" if config[:user]
      load_cmd << " --password=#{config[:pass]}" if config[:pass]
      load_cmd << " --host=#{config[:host]}" if config[:host]
      load_cmd << " #{config[:dbname]}"
      load_cmd
    end

    def mysql2_dump(config)
      dump_cmd = "mysqldump"
      dump_cmd << " --quick --single-transaction"
      dump_cmd << " --user=#{config[:user]}" if config[:user]
      dump_cmd << " --password=#{config[:pass]}" if config[:pass]
      dump_cmd << " --socket=#{config[:socket]}" if config[:socket]
      dump_cmd << " --host=#{config[:host]}" if config[:host]
      dump_cmd << " --opt #{config[:dbname]} #{config[:tables]}"
      dump_cmd
    end

    def mysql2_load(config)
      load_cmd = "mysql"
      load_cmd << " --user=#{config[:user]}" if config[:user]
      #load_cmd << " --password=#{config[:pass]}" if config[:pass]
      load_cmd << " --socket=#{config[:socket]}" if config[:socket]
      load_cmd << " --host=#{config[:host]}" if config[:host]
      load_cmd << " #{config[:dbname]}"
      load_cmd
    end
  end

  class Dir
    include Helper
    attr_accessor :from, :to, :cap
    def initialize(cap, from, to)
      self.from = from
      self.to   = to
      self.cap  = cap
    end

    def sync
      check_deps
      ssh_cmd, server = get_ssh_command
      log "rsync #{server}:#{from} -> local:#{to} (see progress)"
      cmd = "#{ssh_cmd} \"tar -cC #{from} .\" |pv -s #{total} | tar -x -C #{to}"
      cmd = [rsync_command, cat_files_command, pv_command, trash_output_command].join(" | ")
      system cmd
    end

    private

    def rsync_command
      server = cap.find_servers.first
      rsh = "ssh"
      rsh = "#{rsh} -p #{server.port}" if server.port
      cmd = "rsync --verbose --archive --compress --copy-links --delete --rsh='#{rsh}'"
      cmd << " #{cap[:user]}@#{server.host}:#{from}/"
      cmd << " #{to}/ 2> /dev/null"
    end

    def cat_files_command
      t = to.strip.to_s.gsub(/\/$/, '')
      %{ruby -e "p=l='_' ; begin ; l.strip! ; puts File.read('#{t}/'+p) if File.file?('#{t}/'+p) ; p=l ; end  while l=gets"}
    end

    def pv_command
      "pv -s #{total}"
    end

    def trash_output_command
      "cat > /dev/null"
    end

    def total
      unless @total
        log "calculate files size"
        @total = _capture("du -sb #{from} | awk '{print $1}'", :once => true).to_i
        log "TOTAL: #{proxy.number_to_human_size @total}"
      end
      @total
    end

    def proxy
      @proxy ||= HelperProxy.new
    end

    def log(*args)
      cap.logger.info *args
    end
  end

  class HelperProxy
    include ActionView::Helpers::NumberHelper
  end

  module Capistrano
    def self.load_into(configuration)
      configuration.load do
        namespace :sync do
          namespace :dir do
            desc "sync directory"
            task :default, :roles => :app do
              if (ENV["FROM"].blank? || ENV["TO"].blank?)
                puts "Usage cap sync:dir FROM=<..> TO=<..>"
                exit(1)
              end
              from_path = deploy_to + "/" + ENV["FROM"]
              to_path = ENV["TO"]
              s = CapistranoSyncTask::Dir.new(self, from_path, to_path)
              s.sync
            end

            desc "sync public/system directory"
            task :public_system, :roles => :app do
              s = CapistranoSyncTask::Dir.new(self, "#{deploy_to}/current/public/system/", "public/system/")
              s.sync
            end
          end

          desc "sync databases"
          task :db, :roles => :db do
            local_rails_env = ENV["RAILS_ENV"] || "development"
            tables = ENV["TABLES"] || ""
            s = CapistranoSyncTask::Db.new(self, rails_env, local_rails_env, tables)
            s.sync
          end
        end
      end
    end
  end
end

if Capistrano::Configuration.instance
  CapistranoSyncTask::Capistrano.load_into(Capistrano::Configuration.instance)
end
