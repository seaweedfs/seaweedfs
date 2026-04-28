package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	CONNECTION_URL_PATTERN = "%s:%s@tcp(%s:%d)/%s?collation=utf8mb4_bin"
)

func init() {
	filer.Stores = append(filer.Stores, &MysqlStore{})
}

type MysqlStore struct {
	abstract_sql.AbstractSqlStore
}

func (store *MysqlStore) GetName() string {
	return "mysql"
}

func (store *MysqlStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"dsn"),
		configuration.GetString(prefix+"upsertQuery"),
		configuration.GetBool(prefix+"enableUpsert"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"hostname"),
		configuration.GetInt(prefix+"port"),
		configuration.GetString(prefix+"database"),
		configuration.GetInt(prefix+"connection_max_idle"),
		configuration.GetInt(prefix+"connection_max_open"),
		configuration.GetInt(prefix+"connection_max_lifetime_seconds"),
		configuration.GetBool(prefix+"interpolateParams"),
		configuration.GetBool(prefix+"enable_tls"),
		configuration.GetString(prefix+"ca_crt"),
		configuration.GetString(prefix+"client_crt"),
		configuration.GetString(prefix+"client_key"),
		configuration.GetBool(prefix+"tls_insecure_skip_verify"),
		configuration.GetString(prefix+"tls_server_name"),
	)
}

func (store *MysqlStore) initialize(dsn string, upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database string, maxIdle, maxOpen,
	maxLifetimeSeconds int, interpolateParams bool, enableTls bool, caCrtDir string, clientCrtDir string, clientKeyDir string,
	tlsInsecureSkipVerify bool, tlsServerName string) (err error) {

	store.SupportBucketTable = false
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &SqlGenMysql{
		CreateTableSqlTemplate: "",
		DropTableSqlTemplate:   "DROP TABLE `%s`",
		UpsertQueryTemplate:    upsertQuery,
	}

	store.RetryableErrorCallback = func(err error) bool {
		var mysqlError *mysql.MySQLError
		if errors.As(err, &mysqlError) {
			if mysqlError.Number == 1213 { // ER_LOCK_DEADLOCK
				return true
			}
			if mysqlError.Number == 1205 { // ER_LOCK_WAIT_TIMEOUT
				return true
			}
		}
		return false
	}

	if dsn == "" {
		dsn = fmt.Sprintf(CONNECTION_URL_PATTERN, user, password, hostname, port, database)
		if interpolateParams {
			dsn += "&interpolateParams=true"
		}
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("can not parse DSN error:%w", err)
	}

	if enableTls {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: tlsInsecureSkipVerify,
			ServerName:         tlsServerName,
		}

		// When ca_crt is empty, leave RootCAs nil so Go falls back to the
		// system trust store. This is the common case for managed databases
		// (RDS, Aiven, ...) whose certs chain to a public CA already on the host.
		if caCrtDir != "" {
			rootCertPool := x509.NewCertPool()
			pem, err := os.ReadFile(caCrtDir)
			if err != nil {
				return fmt.Errorf("read ca_crt %s: %w", caCrtDir, err)
			}
			if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
				return fmt.Errorf("failed to append root certificate from %s", caCrtDir)
			}
			tlsConfig.RootCAs = rootCertPool
		}

		// Only attempt to load a client keypair when at least one of the paths is
		// set. If either is set, both must load successfully — silently skipping
		// a typo'd path used to mask broken mTLS setups as confusing handshake
		// failures.
		if clientCrtDir != "" || clientKeyDir != "" {
			cert, err := tls.LoadX509KeyPair(clientCrtDir, clientKeyDir)
			if err != nil {
				return fmt.Errorf("load mysql client keypair (crt=%s key=%s): %w", clientCrtDir, clientKeyDir, err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Set TLS directly on the parsed Config rather than registering a global
		// "mysql-tls" entry — the global registry is process-wide and would be
		// overwritten if a second MysqlStore is initialized with different TLS
		// settings.
		cfg.TLS = tlsConfig
	}

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return fmt.Errorf("can not create mysql connector for %s error:%w", maskedDSN(cfg), err)
	}

	store.DB = sql.OpenDB(connector)
	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", maskedDSN(cfg), err)
	}

	return nil
}

func maskedDSN(cfg *mysql.Config) string {
	if cfg.Passwd == "" {
		return cfg.FormatDSN()
	}
	return strings.ReplaceAll(cfg.FormatDSN(), cfg.Passwd, "<ADAPTED>")
}
