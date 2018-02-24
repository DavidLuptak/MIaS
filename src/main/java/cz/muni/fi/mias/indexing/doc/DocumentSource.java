package cz.muni.fi.mias.indexing.doc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * DocumentSource is a source an indexed document can be from. Its stream
 * can be reset and also provides creation of the default Lucene document
 * for this source.
 *
 * _UPDATE_ Instead of creating Lucene document, a mapping for ES's
 *          IndexRequest is created
 * @author mato
 */
public interface DocumentSource {

    // Lucene leftover
    InputStream resetStream() throws IOException;

    // we will be creating mapping below instead of Lucene Document
    // public Document createDocument();

    // Creates the mapping of data as a source for IndexRequest
    // in Map which gets automatically converted to JSON format
    Map<String, Object> createMapping();

    String getDocumentSourcePath();
        
}
