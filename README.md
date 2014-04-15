es_fdr
======

Overview
--------
Reads Lucene Field Data Files (fdt format) created by Elastic Search snapshots.

If your backup (snapshot) goes bad (like you're missing the metadata file) it's very difficult to recover the data - this will read only Lucene FDT file and print them out in the following format:
<type#something|something>
<record_payload>
...


TODO
----
* Add support for compound files (only supports FDT format).
