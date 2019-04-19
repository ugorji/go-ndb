package ndb

/*
Full Text Search support is simple for our use case.
It is model'ed with inspiration from Lucene and From GAE Search.

For each document, we read it and store the following key/value pair:
  - [fieldName][fieldCategory][term][count][docId] ==> [position]...
  - [docId] ==> IndexPaths, ...
    This allows us easily remove documents from the index

Glossary:
  - A document is a {Id int64, []Field}
  - A Field is a {Type int, Key, Value string}.
    Field types are: PlainText, Html, Xml, Atom, Number, Date, GeoPt etc.
    However, fieldCategory as stored in index is one of: Text,Number,Date,GeoPt.
    From a Text/HTML/Xml field, we can detect host names, url's, emails, abbreviations, etc.
  - A document field is decomposed into tokens (terms) for storing in the FTS index.
    The raw document is not stored in the FTS Index.
    However, the terms are stemmed and are void of stop words.
    - Stop words are common words like "the", "a", "is", etc.
    - Stemming involves mapping words like "drive", "driven", "drove", "drives", etc to "drive".

The design of the index allows the following:
  - find best matches for a term without looking at the values
  - build more functionality above the simple index e.g.
    - Clustering to find "phrase" matches
    - Find best matches
    - Build composite queries (e.g. find AND, OR, NOT matches)

Initial Feature set:
  - Incremental add to index
    Add some fields, replaceAll, etc
  - Programatic query index
    AND, OR, NOT support
  - Query results include matches with associated scores and counts
  - Find all terms for a given field
  - Find all documents for a given field/term
  - Remove all documents for a given field/term combination

For help, look at:
  - http://en.wikipedia.org/wiki/Full_text_search
  - http://swtch.com/~rsc/regexp/regexp4.html
  - http://code.google.com/p/codesearch/
  - https://developers.google.com/appengine/docs/python/search/overview

*/

