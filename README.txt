The active objects project uses maven for its build. To build:

1. Install maven (2.2.1 or above):
- you can find all about maven at http://maven.apache.org/
- install instructions are at http://maven.apache.org/download.html#Installation

2. Use the libraries from the 'repositories' directory by either:
- Adding this directory as a repository to your maven configuration, or
- Copying the content of that repository to your local maven repository: ~/.m2/repository

3. From the root of the project, run: mvn install

4. That's it! You will find the active objects library under ${basedir}/activeobjects/target/activeobject-<version>.jar

test
