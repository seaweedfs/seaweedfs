-- Read a SeaweedFS Lance table from DuckDB.
--
-- The lance extension reaches the data over S3 rather than through the
-- namespace, so what is exercised here is the layout on the S3 door and the
-- credentials, not the catalog protocol. That is the point: this is the client
-- that proves a Lance table stays readable without a catalog at all.
--
-- Placeholders are substituted by the Go harness: __ENDPOINT__, __KEY__,
-- __SECRET__, __TABLE__ (s3://bucket/ns/table), __SUFFIXED__ (the same data at a
-- path ending in .lance).

INSTALL lance;
LOAD lance;

CREATE SECRET seaweedfs (
    TYPE lance,
    ACCESS_KEY_ID '__KEY__',
    SECRET_ACCESS_KEY '__SECRET__',
    REGION 'us-east-1',
    ENDPOINT '__ENDPOINT__',
    ALLOW_HTTP true,
    VIRTUAL_HOSTED_STYLE_REQUEST false
);

-- 1. The table the catalog created, read by URI. A table bucket's layout is a
--    valid Lance dataset directory, which is what makes this possible.
SELECT 'scan_rows=' || count(*) FROM __lance_scan('__TABLE__');

-- 2. Its schema survived, vector column included.
SELECT 'scan_columns=' || string_agg(column_name, ',')
FROM (DESCRIBE SELECT * FROM __lance_scan('__TABLE__'));

-- 3. A filter, so it is not only a full scan.
SELECT 'filtered_rows=' || count(*) FROM __lance_scan('__TABLE__') WHERE id < 5;

-- 4. Vector search, which is what the format is for. No index is built here, so
--    this is a brute-force search; the ids nearest the query are what matters.
SELECT 'nearest=' || string_agg(id::VARCHAR, ',')
FROM lance_vector_search('__TABLE__', 'vector',
                         [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], k := 3);

-- 5. DuckDB's replacement scan recognises a path by its .lance suffix. Tables
--    this catalog creates deliberately have no suffix - the name is the table's
--    name, and a suffix would leak into ARNs and policies - so the bare
--    SELECT ... FROM 's3://…' form does not see them, and __lance_scan is the
--    way in. This asserts both halves so that a change upstream is noticed.
SELECT 'suffixed_rows=' || count(*) FROM '__SUFFIXED__';
