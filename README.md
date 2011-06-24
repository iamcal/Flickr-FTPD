An experimental upload-via-FTP interface for Flickr
===================================================

In the very early days of flickr, I wrote this FTPD so that we could have drag and drop uploading via Windows Explorer.

It's non functional because:

1. The ingest endpoint does not work
2. Flickr doesn't store hashed passwords any more (and they were never hashes like the code now implies anyway)
3. There is no SQL escaping
4. Probably other issues

It is of historical interest, if any.


Can this be fixed?
------------------

You could bring this back in a reasonable way and a 3rd party service by first doing OAuth, then embedding the oauth token in the path portion of the FTP URL. Might be a nice proxy service, although who uses FTP these days?