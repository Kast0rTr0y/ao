The active objects project uses maven for its build. To build:

1. Install maven (2.2.1 or above):
- you can find all about maven at http://maven.apache.org/
- install instructions are at http://maven.apache.org/download.html#Installation

2. From the root of the project, run: mvn install

3. That's it! You will find the active objects library under ${basedir}/activeobjects/target/activeobject-<version>.jar

Note: you must use JDK 1.5 to build the project, JDK 1.6 will fail because of changes in the JDBC APIs.
