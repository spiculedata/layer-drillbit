# Overview

Query any non-relational datastore (well, almost...)

Drill supports a variety of NoSQL databases and file systems,
including HBase, MongoDB, MapR-DB, HDFS, MapR-FS, Amazon S3,
Azure Blob Storage, Google Cloud Storage, Swift, NAS and local files.
A single query can join data from multiple datastores. For example,
you can join a user profile collection in MongoDB with a directory
of event logs in Hadoop.

Drill's datastore-aware optimizer automatically restructures a
query plan to leverage the datastore's internal processing capabilities.
In addition, Drill supports data locality, so it's a good idea to
co-locate Drill and the datastore on the same nodes.

# Usage

To deploy this charm simply run:

    juju deploy cs:openjdk
    juju deploy apache-zookeeper zookeeper
    juju add-unit -n 2 apache-zookeeper (optional but recommended for a quorum)
    juju deploy cs:~spicule/drillbit

(If you run this on LXD Local, check the issues below)

Currently there isn't much in the way of actions and relations support,
this will come shortly.

## MongoDB Connectivity

If you are running a Juju hosted MongoDB charm, you can test the MongoDB
SQL support, by running:

    juju add-relation mongodb drillbit

This will create a new storage entry on your drill cluster with connections
to your MongoDB cluster.

To query it you can either connect to drill via JDBC or

    juju ssh drillbit/0
    sudo -i
    cd /opt/drill/bin
    ./drill-conf
    show databases

You should see a connection called something like: juju_mongo_mongodb.<mongodbname>.

Now you can do:

    use juju_mongo_mongodb.Northwind;
    show tables
    select * from mytable;

## Scale out Usage

You can simply add new units and they will be added to the cluster automatically:

    juju add-unit -n 2 drillbit

## Known Limitations and Issues

If you run this on LXD Local there is a bug where its not setting the hostname
of the LXD container and Drill fails to start. For now you need to edit /etc/hosts
and add the hostname to the localhost line ensuring that

    hostname -f

resolves. Once that works:

    cd /opt/drill/bin
    ./drillbit start

# Configuration

drill_url: Allows you to set an alternative download url for Apache Drill.

cluster_id: Allows you to set an alternative cluster id for Zookeeper.

# Contact Information

## DrillBit

  - https://drill.apache.org
  - https://github.com/buggtb/layer-drillbit
  - Contact: tom@analytical-labs.com


[service]: http://example.com
[icon guidelines]: https://jujucharms.com/docs/stable/authors-charm-icon
