package cz.muni.fi.mias.indexing;

import cz.muni.fi.mias.Settings;
import cz.muni.fi.mias.indexing.doc.FileExtDocumentHandler;
import cz.muni.fi.mias.indexing.doc.FolderVisitor;
import cz.muni.fi.mias.indexing.doc.RecursiveFileVisitor;
import cz.muni.fi.mias.math.MathTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Indexing class responsible for adding, updating and deleting files from index,
 * creating, deleting whole index, printing statistics.
 *
 * @author Martin Liska
 * @since 14.5.2010
 */
public class Indexing {

    private static final Logger LOG = LogManager.getLogger(Indexing.class);

    private File indexDir;
    private Analyzer analyzer = new StandardAnalyzer();
    private long docLimit = Settings.getDocLimit();
    private long count = 0;
    private long progress = 0;
    private long fileProgress = 0;
    private String storage;
    private long startTime;

    private RestHighLevelClient client;
    private String indexName;
    private String type;

    /**
     * Constructor creates Indexing instance. Directory with the index is taken from the Settings.
     *
     */
    public Indexing(RestHighLevelClient client) {
        this.indexDir = new File(Settings.getIndexDir());
        this.client = client;
        this.indexName = "mias-index";
        this.type = "mias-math-document";
    }

    // ES index
    public void createIndex() {
        // for testing
        deleteIndex();

        CreateIndexRequest request = new CreateIndexRequest(indexName);

        // define analyzer / tokenizer and mapping for 'pmath'
        request.source(
                "{\n" +
                    "\"settings\":{\n" +
                        "\"analysis\":{\n" +
                            "\"analyzer\":{\n" +
                                "\"miasmath-analyzer\":{\n" +
                                    "\"type\":\"custom\",\n" +
                                    "\"tokenizer\": \"miasmath-tokenizer\"\n" +
                                "}\n" +
                            "}\n" +
                        "}\n" +
                    "},\n" +
                    "\"mappings\": {\n" +
                        "\"mias-math-document\": {\n" +
                            "\"properties\": {\n" +
                                "\"pmath\": {\n" +
                                    "\"type\": \"text\",\n" +
                                    "\"analyzer\": \"miasmath-analyzer\",\n" +
                                    "\"search_analyzer\": \"miasmath-analyzer\"\n" +
                                "}\n" +
                            "}\n" +
                        "}\n" +
                    "}\n" +
                        "}",
                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ES index
    private void deleteIndex() {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            client.indices().delete(request);
            LOG.info("Index '{}' successfully deleted", indexName);
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                LOG.warn("Index '{}' not found to delete", indexName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Indexes files located in given input path.
     * @param path Path to the documents directory. Can be a single file as well.
     * @param rootDir A path in the @path parameter which is a root directory for the document storage. It determines the relative path
     * the files will be index with.
     */
    public void indexFiles(String path, String rootDir) {
        storage = rootDir;
        if (!storage.endsWith(File.separator)) {
            storage += File.separator;
        }
        final File docDir = new File(path);
        if (!docDir.exists() || !docDir.canRead()) {
            LOG.fatal("Document directory '{}' does not exist or is not readable, please check the path.",docDir.getAbsoluteFile());
            System.exit(1);
        }
        try {
            startTime = System.currentTimeMillis();
            // Lucene leftover:
            // IndexWriterConfig config = new IndexWriterConfig(analyzer);
            // PayloadSimilarity ps = new PayloadSimilarity();
            // ps.setDiscountOverlaps(false);
            // config.setSimilarity(ps);
            // config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
            // try (IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir.toPath()), config))
            // {
            LOG.info("Getting list of documents to index.");
            List<File> files = getDocs(docDir);
            countFiles(files);
            LOG.info("Number of documents to index is {}",count);
            indexDocsThreaded(files);
            // }
        } catch (IOException ex) {
            LOG.error(ex);
        }
    }

    private List<File> getDocs(File startPath) throws IOException {
        if(!startPath.canRead())
        {
            throw new IllegalArgumentException("Given path is not a folder. # "+startPath);
        }
        else
        {
            RecursiveFileVisitor fileVisitor = new FolderVisitor(docLimit);
            Files.walkFileTree(startPath.toPath(), fileVisitor);
            // TODO remove later
            List<File> result = new ArrayList<>(fileVisitor.getVisitedPaths().size());
            for(Path p : fileVisitor.getVisitedPaths())
            {
                result.add(p.toFile());
            }
            
            return result;
        }
    }

    // ES indexing
    private void indexDocsSync(List<Map<String, Object>> mappings) throws IOException {
        for (Map<String, Object> map : mappings) {
            IndexRequest indexRequest = new IndexRequest(indexName, type);
            indexRequest.source(map);

            LOG.info("adding to index {} docId={}",
                    indexRequest.sourceAsMap().get("path"),
                    indexRequest.sourceAsMap().get("id"));

            // sync call
            try {
                IndexResponse indexResponse = client.index(indexRequest);
                if ((indexResponse.getResult() == DocWriteResponse.Result.CREATED) ||
                    (indexResponse.getResult() == DocWriteResponse.Result.UPDATED)) {
                    LOG.info("Doc '{}' indexed", indexResponse.toString());
                    LOG.info("Documents indexed: {}", ++progress);
                }
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    LOG.error("Elasticsearch exception while indexing");
                    LOG.error(e.getDetailedMessage(), e);
                }
            }
        }
    }

    // ES indexing
    private void indexDocsAsync(List<Map<String, Object>> mappings) {

        for (Map<String, Object> map : mappings) {
            IndexRequest indexRequest = new IndexRequest(indexName, type);
            indexRequest.source(map);

            LOG.info("adding to index {} docId={}",
                    indexRequest.sourceAsMap().get("path"),
                    indexRequest.sourceAsMap().get("id"));

            // async call
            client.indexAsync(indexRequest, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    LOG.info("Successfully indexed.");
                    LOG.info("Documents indexed: {}", ++progress);
                }

                @Override
                public void onFailure(Exception e) {
                    LOG.fatal("Indexing failed");
                    LOG.fatal("Document '{}' indexing failed: {}",
                            indexRequest.sourceAsMap().get("path"),
                            e.getMessage());
                    // get also stack trace
                    LOG.error(e.getMessage(), e);
                }
            });
        }
    }

    // ES indexing
    private void indexDocsBulk(List<Map<String, Object>> mappings) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        for (Map<String, Object> map : mappings) {
            IndexRequest indexRequest = new IndexRequest(indexName, type);
            indexRequest.source(map);

            bulkRequest.add(indexRequest);

            LOG.info("adding to index {} docId={}",
                    indexRequest.sourceAsMap().get("path"),
                    indexRequest.sourceAsMap().get("id"));

        }

        // sync call
        BulkResponse bulkResponse = client.bulk(bulkRequest);

        // async call
//        client.bulkAsync(bulkRequest, new ActionListener<BulkResponse>() {
//            @Override
//            public void onResponse(BulkResponse bulkItemResponses) {
//                LOG.info("Successfully indexed.");
//                LOG.info("Documents indexed: {}", ++progress);
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                LOG.fatal("Indexing failed");
//                LOG.error(e.getMessage(), e);
//            }
//        });
    }

    // ES indexing
    private void indexDocsNonThreaded(List<File> files) {

        try {
            for (File file : files) {
                FileExtDocumentHandler fileExtDocumentHandler = new FileExtDocumentHandler(file, resolvePath(file));
                List<Map<String, Object>> mappings = fileExtDocumentHandler.getMappings(file, resolvePath(file));

                indexDocsSync(mappings);
                //indexDocsAsync(mappings);
                //indexDocsBulk(mappings);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void indexDocsThreaded(List<File> files) {
        try {
            Iterator<File> it = files.iterator();
            ExecutorService executor = Executors.newFixedThreadPool(Settings.getNumThreads());
            Future[] tasks = new Future[Settings.getNumThreads()];
            int running = 0;

            while (it.hasNext() || running > 0) {
                for (int i = 0; i < tasks.length; i++) {
                    if (tasks[i] == null && it.hasNext()) {
                        File f = it.next();
                        String path = resolvePath(f);
                        Callable callable = new FileExtDocumentHandler(f, path);
                        FutureTask ft = new FutureTask(callable);
                        tasks[i] = ft;
                        executor.execute(ft);
                        running++;
                    } else if (tasks[i] != null && tasks[i].isDone()) {
                        List<Map<String, Object>> mappings = (List<Map<String, Object>>) tasks[i].get();
                        running--;
                        tasks[i] = null;
                        indexDocsSync(mappings);
                        LOG.info("File progress: {} of {} done...",++fileProgress, count);
                    }
                }
            }
            printTimes();
            executor.shutdown();
        } catch (IOException | InterruptedException | ExecutionException ex) {
            LOG.fatal(ex);
        }
    }

    /**
     * Optimizes the index.
     */
    public void optimize() {        
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        // TODO what do we measure here ? time of optimization or optimiziation
        // and index opening aswell
        startTime = System.currentTimeMillis();
        try(IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir.toPath()), config)){
//            writer.optimize();    
            LOG.info("Optimizing time: {} ms",System.currentTimeMillis()-startTime);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Deletes whole current index directory
     */
    public void deleteIndexDir() {
        deleteDir(indexDir);
    }

    private void deleteDir(File f) {
        if (f.exists()) {
            File[] files = f.listFiles();
            for (File file : files)
            {
                if (file.isDirectory())
                {
                    deleteDir(file);
                }
                else
                {
                    file.delete();
                }
            }
            f.delete();
        }
    }

    /**
     * Deletes files located in given path from the index
     *
     * @param path Path of the files to be deleted
     */
    public void deleteFiles(String path) {
        final File docDir = new File(path);
        if (!docDir.exists() || !docDir.canRead()) {
            LOG.error("Document directory '{}' does not exist or is not readable, please check the path.", docDir.getAbsolutePath());
            System.exit(1);
        }
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        try(IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir.toPath()), config)) {
            deleteDocs(writer, docDir);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private void deleteDocs(IndexWriter writer, File file) throws IOException {
        if (file.canRead()) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File file1 : files)
                    {
                        deleteDocs(writer, file1);
                    }
                }
            } else {
                LOG.info("Deleting file {}.",file.getAbsoluteFile());
                writer.deleteDocuments(new Term("path",resolvePath(file)));
            }
        }
    }

    /**
     * Prints statistic about the current index
     */
    public void getStats() {
        String stats = "\nIndex statistics: \n\n";
        try(DirectoryReader dr = DirectoryReader.open(FSDirectory.open(indexDir.toPath()))) {
            stats += "Index directory: "+indexDir.getAbsolutePath() + "\n";
            stats += "Number of indexed documents: " + dr.numDocs() + "\n";
            
            long fileSize = 0;
            for (int i = 0; i < dr.numDocs(); i++) {
                Document doc = dr.document(i);
                if (doc.getField("filesize")!=null) {
                    String size = doc.getField("filesize").stringValue();
                    fileSize += Long.valueOf(size);
                }
            }
            long indexSize = 0;
            File[] files = indexDir.listFiles();
            for (File f : files) {
                indexSize += f.length();
            }
            stats += "Index size: " + indexSize + " bytes \n";
            stats += "Approximated size of indexed files: " + fileSize + " bytes \n";

            LOG.info(stats);
        } catch (IOException | NumberFormatException e) {
            LOG.error(e.getMessage());
        } 
    }



    private String resolvePath(File file) throws IOException {
        String path = file.getCanonicalPath();
        return path.substring(storage.length());
    }

    private long getCpuTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long result = 0;
        if (bean.isThreadCpuTimeSupported()) {
            final long[] ids = bean.getAllThreadIds();
            for (long id : ids) {
                result += bean.getThreadCpuTime(id) / 1000000;
            }
        }
        return result;
    }

    private long getUserTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long result = 0;
        if (bean.isThreadCpuTimeSupported()) {
            final long[] ids = bean.getAllThreadIds();
            for (long id : ids) {
                result += bean.getThreadUserTime(id) / 1000000;
            }
        }
        return result;
    }

    private void printTimes() {
        LOG.info("---------------------------------");
        LOG.info(Settings.EMPTY_STRING);
        LOG.info("{} DONE in total time {} ms",progress,System.currentTimeMillis() - startTime);
        LOG.info("CPU time {} ms",getCpuTime());
        LOG.info("user time {} ms",getUserTime());
        MathTokenizer.printFormulaeCount(); // TODO
        LOG.info(Settings.EMPTY_STRING);
    }

    private void countFiles(List<File> files) {
        if (docLimit > 0) {
            count = Math.min(files.size(), docLimit);
        } else {
            count = files.size();
        }
    }
}
