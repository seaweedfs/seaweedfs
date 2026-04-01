Populate data run:

  - make -C test/s3tables help
  - make -C test/s3tables populate-trino
  - make -C test/s3tables populate-spark

  Run:

  - make -C test/s3tables populate
  - If your account id differs, override: make -C test/s3tables populate
    TABLE_ACCOUNT_ID=000000000000
