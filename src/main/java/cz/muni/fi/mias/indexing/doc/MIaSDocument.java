/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.mias.indexing.doc;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for classes that create Lucene documents.
 *
 * _UPDATE_ ..creates a mapping for ES's IndexRequest..
 * 
 * @author Martin Liska
 */
public interface MIaSDocument {

    List<Map<String, Object>> getMappings() throws IOException;

    String getLogInfo();
}
