# elasticrawl-examples

Example Hadoop jobs demonstrating the Elasticrawl tool.  [Elasticrawl](https://github.com/rossf7/elasticrawl)
is a tool for launching AWS Elastic MapReduce jobs against the [Common Crawl](http://commoncrawl.org) corpus.

## Jobs

* WordCount - An implementation of the standard Hadoop Word Count example that
parses text data in Common Crawl WET (WARC Encoded Text) files. Each WordCount
job parses a single segment of Common Crawl data.

* SegmentCombiner - Combines data from multiple Common Crawl segments to produce a
single set of results.

## Running with Elasticrawl

See [http://github.com/rossf7/elasticrawl#Quick-Start](http://github.com/rossf7/elasticrawl#Quick-Start)

## Building

Developed on Ubuntu 12.04 and OpenJDK 6 using Eclipse Kepler and
the m2e plugin.

### with Maven

```
git clone https://github.com/rossf7/elasticrawl-examples.git
cd elasticrawl-examples
mvn install
```

### with Eclipse

```
cd ~/workspace
git clone https://github.com/rossf7/elasticrawl-examples.git
```

* Open Eclipse
* File --> Import
* Maven --> Existing Maven Project
* Run As --> Maven install

## Links

* [Common Crawl 2013 data structure and file formats](http://commoncrawl.org/new-crawl-data-available/)

## Thanks

* Mark Watson for his [example-warc-java](https://github.com/commoncrawl/example-warc-java).
* Lemur project developers for their edu.cmu.lemurproject package.  Source for this is included
with a couple of minor changes needed to process WET files stored on S3.

## License

This code is licensed under the MIT license.
