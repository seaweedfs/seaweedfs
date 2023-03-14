gen:
	for lang in cpp-restsdk csharp go kotlin-client php python-nextgen ruby; do \
  		openapi-generator generate -i master.yaml -g $$lang -o ../master-clients/$$lang; \
	done