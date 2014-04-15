es_fdr
======

Reads Lucene Field Data Files (fdt format) created by Elastic Search snapshots.

If your backup (snapshot) goes bad (like you're missing the metadata file) it's very difficult to recover the data - this tool will read any file and if it's the correct format prints the data out in the following format:
```
<index_type#unique_record_identifier>
<record_payload>
...
```

TODO
----
* Add support for compound files (only supports FDT format).
