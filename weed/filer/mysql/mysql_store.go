package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"

	_ "github.com/go-sql-driver/mysql"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	CONNECTION_URL_PATTERN     = "%s:%s@tcp(%s:%d)/%s?collation=utf8mb4_bin"
	CONNECTION_TLS_URL_PATTERN = "%s:%s@tcp(%s:%d)/%s?collation=utf8mb4_bin&tls=mysql-tls"
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
	)
}

func (store *MysqlStore) initialize(dsn string, upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database string, maxIdle, maxOpen,
	maxLifetimeSeconds int, interpolateParams bool, enableTls bool, caCrtDir string, clientCrtDir string, clientKeyDir string) (err error) {

	store.SupportBucketTable = false
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &SqlGenMysql{
		CreateTableSqlTemplate: "",
		DropTableSqlTemplate:   "DROP TABLE `%s`",
		UpsertQueryTemplate:    upsertQuery,
	}

	if enableTls {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(caCrtDir)
		if err != nil {
			return err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return fmt.Errorf("failed to append root certificate")
		}

		clientCert := make([]tls.Certificate, 0)
		if cert, err := tls.LoadX509KeyPair(clientCrtDir, clientKeyDir); err == nil {
			clientCert = append(clientCert, cert)
		}

		tlsConfig := &tls.Config{
			RootCAs:      rootCertPool,
			Certificates: clientCert,
			MinVersion:   tls.VersionTLS12,
		}
		err = mysql.RegisterTLSConfig("mysql-tls", tlsConfig)
		if err != nil {
			return err
		}
	}

	if dsn == "" {
		pattern := CONNECTION_URL_PATTERN
		if enableTls {
			pattern = CONNECTION_TLS_URL_PATTERN
		}
		dsn = fmt.Sprintf(pattern, user, password, hostname, port, database)
		if interpolateParams {
			dsn += "&interpolateParams=true"
		}
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("can not parse DSN error:%v", err)
	}

	var dbErr error
	store.DB, dbErr = sql.Open("mysql", dsn)
	if dbErr != nil {
		store.DB.Close()
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", strings.ReplaceAll(dsn, cfg.Passwd, "<ADAPTED>"), err)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", strings.ReplaceAll(dsn, cfg.Passwd, "<ADAPTED>"), err)
	}

	return nil
}
