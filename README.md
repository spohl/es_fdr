es_fdr
======

Reads Lucene Field Data Files (fdt format) created by Elastic Search snapshots.

If your backup (snapshot) goes bad (like you're missing the metadata file) it's very difficult to recover the data.  This tool will read any file and, if it's the correct format, print out the data in the following format:
```
<index_type#unique_record_identifier>
<record_payload>
...
```

TODO
----
* Support for compound files (only supports FDT format).
* Support data spread across multiple parts (__a1.part0 ...) - at the moment you have to concatenate the files together and there seems to be problems with files > 100MB.
