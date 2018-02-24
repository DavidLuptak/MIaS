/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.mias.indexing.doc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * File implementation of the DocumentSource.
 * 
 * @author Martin Liska
 */
public class FileDocument implements DocumentSource {
    
    private File file;
    private String path;

    /**
     * @param file File from which this DocumentSource is created.
     * @param path Relative path to the file.
     */
    public FileDocument(File file, String path) {
        this.file = file;
        this.path = path;
    }
    
    @Override
    public InputStream resetStream() throws IOException {
       return new FileInputStream(file); 
    }
    
    /**
     * Creates Lucene document with the fields:
     * <ul>
     *  <li>path: relative path from the constructor</li>
     *  <li>id: the same as path</li>
     *  <li>modified: last modified date of the file</li>
     *  <li>filesize: size of the file</li>
     *  <li>title: name of the file</li>
     * </ul>
     * @return New Lucene document.
     */
//    @Override
//    public Document createDocument() {
//        Document doc = new Document();
//
//        doc.add(new StringField("path", path, Field.Store.YES));
//
//        doc.add(new StringField("id", path, Field.Store.YES));
//
//        doc.add(new StringField("modified",
//                DateTools.timeToString(file.lastModified(), DateTools.Resolution.MINUTE),
//                Field.Store.YES));
//
//        doc.add(new LongField("filesize", file.length(), Field.Store.YES));
//
//        doc.add(new TextField("title", file.getName(), Field.Store.YES));
//        return doc;
//    }

    /**
     * Creates Elasticsearch IndexRequest mapping with the fields same as above.
     *
     * @return New Elasticsearch IndexRequest mapping
     */
    @Override
    public Map<String, Object> createMapping() {

        Map<String, Object> mapping = new HashMap<>();

        mapping.put("path", path);
        mapping.put("id", path);
        mapping.put("modified", file.lastModified()); // unix time is probably ok for now
        mapping.put("filesize", file.length());
        mapping.put("title", file.getName());

        return mapping;
    }


    @Override
    public String getDocumentSourcePath() {
        return path;
    }

}
