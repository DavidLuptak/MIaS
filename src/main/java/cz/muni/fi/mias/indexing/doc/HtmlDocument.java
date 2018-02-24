package cz.muni.fi.mias.indexing.doc;

import cz.muni.fi.mias.math.MathTokenizer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * MIaSDocument implementation for creating Lucene document from xhtml and html files.
 *
 * _UPDATE_ ..creates a mapping for ES's IndexRequest..
 *
 * @author Martin Liska
 */
public class HtmlDocument extends AbstractMIaSDocument {

    public HtmlDocument(DocumentSource source) {
        super(source);
    }

    @Override
    public List<Map<String, Object>> getMappings() throws IOException {
        Map<String, Object> mapping = source.createMapping();

        HtmlDocumentExtractor htmldoc = new HtmlDocumentExtractor(source.resetStream());

        String arxivId = htmldoc.getArxivId();
        if (arxivId != null) {
            // document.removeField("id");
            // Field arxivIdField = new StringField("id", arxivId, Field.Store.YES);
            // document.add(arxivIdField);
            mapping.put("id", arxivId);
        }

        String title = htmldoc.getTitle();
        if (title != null) {
            // document.removeField("title");
            // Field titleField = new TextField("title", title, Field.Store.YES);
            // titleField.setBoost(Float.parseFloat("10.0"));
            // document.add(titleField);
            mapping.put("title", title);
            // TODO setBoost
        }

        String authors = htmldoc.getAuthors();
        if (authors != null) {
            // Field authorsField = new StringField("authors", authors, Field.Store.YES);
            // authorsField.setBoost(Float.parseFloat("10.0"));
            // document.add(authorsField);
            mapping.put("authors", authors);
            // TODO setBoost
        }

        String content = htmldoc.getBody();
        if (content != null) {
            // document.add(new TextField("content", content, Field.Store.NO));
            mapping.put("content", content);
        }

        InputStreamReader isr = new InputStreamReader(source.resetStream(), "UTF-8");
        // document.add(new TextField("pmath", new MathTokenizer(isr, true, MathTokenizer.MathMLType.PRESENTATION)));

        MathTokenizer pmath = new MathTokenizer(isr, true, MathTokenizer.MathMLType.PRESENTATION);
        mapping.put("pmath", pmath);
        isr = new InputStreamReader(source.resetStream(), "UTF-8");
        // document.add(new TextField("cmath", new MathTokenizer(isr, true, MathTokenizer.MathMLType.CONTENT)));
        MathTokenizer cmath = new MathTokenizer(isr, true, MathTokenizer.MathMLType.CONTENT);
        mapping.put("cmath", cmath);

        return Arrays.asList(mapping);
    }

}
