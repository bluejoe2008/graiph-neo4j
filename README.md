## graiph-neo4j: enhancing Neo4j with BLOB(binary large object) handling 

graiph-neo4j enhances Neo4j with a set of blob operation functions which makes it possible and convenient to store and use the BLOB in neo4j.

### BLOB

BLOBs represent binary streams stored in an EXTERNAL storage (local files system, distributed file system, etc) and can be consumed in streamming manner (only offer new bytes on calling `next()`). Unlike BLOBs, byte arrays are always stored INSIDE a Neo4j store, and often be fetched as a whole object (thus large byte arrays are not suitable to be stored in Neo4j).

A BLOB contains two properties: `length` and `mimeType`:

```
trait Blob extends Comparable[Blob] {
  val length: Long;
  val mimeType: MimeType;
  ...
}
```

`BlobFactory` provides serveral methods to build a blob object:

* def fromBytes(bytes: Array[Byte]): Blob
* val EMPTY: Blob 
* def fromInputStreamSource(iss: InputStreamSource, length: Long, mimeType: Option[MimeType] = None): Blob
* def fromFile(file: File, mimeType: Option[MimeType] = None): Blob
* def fromHttpURL(url: String): Blob
* def fromURL(url: String): Blob

To consume content of a BLOB, use `offerStream`:
```
  blob.offerStream { is =>
    is.read();
    ...
  }
```

It is unrecommended to use `Blob.toByteArray()` to avoid large memory cost.

### building

graiph-neo4j/Neo4j is written in Scala, it requires Scala SDK 2.11 and JDK 8.0.

graiph-neo4j depends on an external library `graiph-blob-common`, which defines interfaces `Blob`, `BlobId`, ...
For integration tests, an other external library `graiph-java-driver` is also required, which provides java driver for Neo4j server.

1. install graiph-blob-common
```
cd <your workspace dir>
git clone https://github.com/grapheco/graiph-blob-common
cd graiph-blob-common
mvn install
```

2. build graiph-java-driver
```
cd <your workspace dir>
git clone https://github.com/grapheco/graiph-java-driver
cd graiph-java-driver
mvn install -DskipTests
```

3. return to `graiph-neo4j` project:
```
mvn install
```

This will compile all Java & Scala source code and build all modules completely.

### writing & reading blobs

```
  //create a node
  val node1 = db.createNode();

  node1.setProperty("name", "bob");
  node1.setProperty("age", 40);

  //with a blob property
  node1.setProperty("photo", BlobFactory.fromFile(new File("./testdata/test.png")));
  ...
```

The code shown above creates a graph node with a `photo` property, which is of a blob type, and its content comes from the local file `./testdata/test.png`.

To read a blob from Neo4j database:
```
  //reload database
  val db2 = openDatabase();
  val tx2 = db2.beginTx();
  //get first node
  val it = db2.getAllNodes().iterator();
  val v1: Node = it.next();
  val blob = v1.getProperty("photo").asInstanceOf[Blob];
```

For more example code, see https://github.com/bluejoe2008/neo4j-blob/blob/blob-support/community/neo4j-blob/neo4j-blob-test/src/test/scala/GraphDbBlobTest.scala

### blob in Cypher

`BlobLiteral` is defined in Cypher grammar in form of:
`<schema://path>`

Following schema is ok:
* file: local files in Neo4j server
* http
* https
* ftp
* base64: path should be a BASE64 encoding string, for example: \<base64://dGhpcyBpcyBhbiBleGFtcGxl\> represents a string with content `this is an example`

Next code illustrates how to use blob in Cypher query:
```
//create a node
CREATE (n:PERSON { name: "bluejoe", photo: <https://avatars0.githubusercontent.com/u/2328905?s=460&v=4> });

```

Retrieving a BLOB property:
```
val result = tx.run("MATCH (n:PERSON) where n.name='bluejoe' return n");
val blob = result.get(0).asBlob
```

For more example code, see https://github.com/bluejoe2008/neo4j-blob/blob/blob-support/community/neo4j-blob/neo4j-blob-test/src/test/scala/BoltCypherTest.scala

### blob functions

Serveral functions are defined as Cypher UDFs:

* Blob.fromFile(filePath: String): Blob
* Blob.fromURL(url: String): Blob
* Blob.fromString(text: String, encoding: String): Blob
* Blob.fromUTF8String(text: String): Blob
* Blob.fromBytes(bytes: Array[Byte]): Blob
* Bytes.guessType(bytes: Array[Byte]): String
* Blob.guessType(blob: Blob): String
* Blob.empty(): Blob
* Blob.len(blob: Blob): Long
* Blob.toString(blob: Blob, encoding: String): String
* Bytes.fromFile(filePath: String): Array[Byte] 
* Blob.toUTF8String(blob: Blob): String
* Blob.toBytes(blob: Blob): Array[Byte]
* Blob.mime(blob: Blob): String, get mime type, e.g. image/png
* Blob.mime1(blob: Blob): String, get major mime type, e.g. image
* Blob.mime2(blob: Blob): String, get minor mime type, e.g. png
* Blob.is(blob: Blob, mimeType: String): Boolean, determine if the blob is kind of specified mime type

These functions are registered automatically on start, so be free to use them.

### Licensing

Following Neo4j Community Edition, graiph-neo4j is an open source product licensed under GPLv3.
