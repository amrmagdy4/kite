Kite: A Microblog Data Management System
=========================================

Kite is an open-source system to query Twitter-like data in real time. Kite is a distributed system that is built on top of Apache Ignite to manage Twitter-like data at scale. If you are working on analyzing or building applications on top of tweets, Kite allows you to seamlessly exploit this data, that comes in huge numbers, without worrying about how to manage the data itself, just connect and fly with Kite. In general, Kite is managing any micro-length data, called Microblogs, not just Twitter data. Kite is tailored for the requirements of Microblogs applications and supports efficient queries on arbitrary attributes of Microblogs to support a myriad of applications.

The main features of Kite includes:
* Connect Microblogs streams of arbitrary attributes and schema from local and remote sources.
* Create index structures on arbitrary attributes of existing Microblogs streams. Kite provides both spatial and non-spatial index types.
* Add and remove machines dynamically to Kite cluster as needed without restarting or interrupting the cluster operation.
* Search existing streams using MQL query language and Java-compatible APIs. Kite automatically chooses the right index structures to access to process queries efficiently.
* Manage and administrate existing streams and index structures with a variety of utility commands and tools.

For information on how to get started with Kite please visit:

    http://kite.cs.umn.edu
