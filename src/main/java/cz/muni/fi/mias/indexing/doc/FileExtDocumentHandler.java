package cz.muni.fi.mias.indexing.doc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Class providing document handling for files based on their file extension.
 * html,xhtml and txt files supported so far.
 *
 * @author Martin Liska
 */
public class FileExtDocumentHandler implements Callable<List<Map<String, Object>>> {

    private static final Logger LOG = LogManager.getLogger(FileExtDocumentHandler.class);
    
    private File file;
    private String path;
    private MIasDocumentFactory mIasDocumentFactory = new MIasDocumentFactory();

    public FileExtDocumentHandler(File file, String path) {
        this.file = file;
        this.path = path;
    }

    /**
     * Calls corresponding document for input files based on it's extension.
     * If needed, extracts an archive for file entries.
     * HtmlDocument is called in case of xhtml, html and xml files.
     * @param file Input file to be handled.
     * @return List of documents mappings for the input files
     */
    public List<Map<String, Object>> getMappings(File file, String path) {
        String ext = path.substring(path.lastIndexOf(".") + 1);
        List<Map<String, Object>> result = new ArrayList<>();
        List<MIaSDocument> miasDocuments = new ArrayList<>();
        try {
            ZipFile zipFile;
            if (ext.equals("zip")) {
                LOG.info("ZIP DOCUMENT HAS BEEN CHOSEN");
                zipFile = new ZipFile(file);
                Enumeration<? extends ZipEntry> e = zipFile.entries();
                while (e.hasMoreElements()) {
                    ZipEntry entry = e.nextElement();
                    if (!entry.isDirectory()) {
                        String name = entry.getName();
                        int extEnd = name.lastIndexOf("#");
                        if (extEnd < name.lastIndexOf(".")) {
                            extEnd = name.length();
                        }
                        ext = name.substring(name.lastIndexOf(".") + 1, extEnd);
                        MIaSDocument miasDocument = mIasDocumentFactory.buildDocument(ext, new ZipEntryDocument(zipFile, path, entry));
                        if (miasDocument != null) {
                            miasDocuments.add(miasDocument);
                        }
                    }
                }
            } else {
                LOG.info("HTML or FORMULA DOCUMENT HAS BEEN CHOSEN");
                DocumentSource source = new FileDocument(file, path);
                MIaSDocument miasDocument = mIasDocumentFactory.buildDocument(ext, source);
                if (miasDocument!=null) {
                    miasDocuments.add(miasDocument);
                }
            }            
            for (MIaSDocument doc : miasDocuments) {
                result.addAll(doc.getMappings());
            }
        } catch (IOException ex) {
            LOG.error("Cannot handle file {}", file.getAbsolutePath());
            LOG.error(ex);
        }
        
        return result;
    }

    public List<Map<String, Object>> call() {
        return getMappings(file, path);
    }
    
}
