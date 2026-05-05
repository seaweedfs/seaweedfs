package unity_catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ucClient struct {
	base string
	http *http.Client
}

func newUCClient(base string) *ucClient {
	return &ucClient{base: base + ucAPIBase, http: &http.Client{Timeout: 30 * time.Second}}
}

type ucCreateCatalog struct {
	Name        string `json:"name"`
	Comment     string `json:"comment,omitempty"`
	StorageRoot string `json:"storage_root,omitempty"`
}

type ucCatalogInfo struct {
	Name        string `json:"name"`
	StorageRoot string `json:"storage_root"`
	ID          string `json:"id"`
}

type ucCreateSchema struct {
	Name        string `json:"name"`
	CatalogName string `json:"catalog_name"`
}

type ucSchemaInfo struct {
	Name        string `json:"name"`
	CatalogName string `json:"catalog_name"`
	FullName    string `json:"full_name"`
	SchemaID    string `json:"schema_id"`
}

type ucColumn struct {
	Name     string `json:"name"`
	TypeText string `json:"type_text"`
	TypeJSON string `json:"type_json,omitempty"`
	TypeName string `json:"type_name"`
	Position int    `json:"position"`
	Nullable bool   `json:"nullable"`
}

type ucCreateTable struct {
	Name             string     `json:"name"`
	CatalogName      string     `json:"catalog_name"`
	SchemaName       string     `json:"schema_name"`
	TableType        string     `json:"table_type"`
	DataSourceFormat string     `json:"data_source_format"`
	Columns          []ucColumn `json:"columns"`
	StorageLocation  string     `json:"storage_location,omitempty"`
}

type ucTableInfo struct {
	Name             string     `json:"name"`
	CatalogName      string     `json:"catalog_name"`
	SchemaName       string     `json:"schema_name"`
	TableType        string     `json:"table_type"`
	DataSourceFormat string     `json:"data_source_format"`
	Columns          []ucColumn `json:"columns"`
	StorageLocation  string     `json:"storage_location"`
	TableID          string     `json:"table_id"`
}

type ucListTablesResp struct {
	Tables []ucTableInfo `json:"tables"`
}

type ucAwsCreds struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
}

type ucTempCreds struct {
	AwsTempCredentials *ucAwsCreds `json:"aws_temp_credentials"`
	ExpirationTime     int64       `json:"expiration_time"`
}

func (c *ucClient) createCatalog(ctx context.Context, in ucCreateCatalog) (*ucCatalogInfo, error) {
	var out ucCatalogInfo
	return &out, c.do(ctx, http.MethodPost, "/catalogs", in, &out)
}

func (c *ucClient) deleteCatalog(ctx context.Context, name string) error {
	q := url.Values{"force": []string{"true"}}
	return c.do(ctx, http.MethodDelete, "/catalogs/"+url.PathEscape(name)+"?"+q.Encode(), nil, nil)
}

func (c *ucClient) createSchema(ctx context.Context, in ucCreateSchema) (*ucSchemaInfo, error) {
	var out ucSchemaInfo
	return &out, c.do(ctx, http.MethodPost, "/schemas", in, &out)
}

func (c *ucClient) deleteSchema(ctx context.Context, fullName string) error {
	q := url.Values{"force": []string{"true"}}
	return c.do(ctx, http.MethodDelete, "/schemas/"+url.PathEscape(fullName)+"?"+q.Encode(), nil, nil)
}

func (c *ucClient) createTable(ctx context.Context, in ucCreateTable) (*ucTableInfo, error) {
	var out ucTableInfo
	return &out, c.do(ctx, http.MethodPost, "/tables", in, &out)
}

func (c *ucClient) getTable(ctx context.Context, fullName string) (*ucTableInfo, error) {
	var out ucTableInfo
	return &out, c.do(ctx, http.MethodGet, "/tables/"+url.PathEscape(fullName), nil, &out)
}

func (c *ucClient) listTables(ctx context.Context, catalog, schema string) ([]ucTableInfo, error) {
	var out ucListTablesResp
	q := url.Values{
		"catalog_name": []string{catalog},
		"schema_name":  []string{schema},
	}
	path := "/tables?" + q.Encode()
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out.Tables, nil
}

func (c *ucClient) deleteTable(ctx context.Context, fullName string) error {
	return c.do(ctx, http.MethodDelete, "/tables/"+url.PathEscape(fullName), nil, nil)
}

func (c *ucClient) generateTemporaryTableCredentials(ctx context.Context, tableID, op string) (*ucTempCreds, error) {
	body := map[string]string{"table_id": tableID, "operation": op}
	var out ucTempCreds
	return &out, c.do(ctx, http.MethodPost, "/temporary-table-credentials", body, &out)
}

func (c *ucClient) do(ctx context.Context, method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal %s %s: %w", method, path, err)
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 10 MiB cap is well above any reasonable Unity Catalog response and
	// keeps a runaway server from OOMing the test runner.
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return fmt.Errorf("read %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}
	if out == nil || len(respBytes) == 0 {
		return nil
	}
	if err := json.Unmarshal(respBytes, out); err != nil {
		return fmt.Errorf("decode %s %s: %w (body=%s)", method, path, err, string(respBytes))
	}
	return nil
}
