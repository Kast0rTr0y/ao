The active objects project uses maven for its build. To build:

1. Install maven:
- you can find all about maven at http://maven.apache.org/
- install instructions are at http://maven.apache.org/download.html#Installation

2. From the root of the project, run: mvn install -s settings.xml

3. That's it! You will find the active objects library under ${basedir}/ActiveObjects/target/activeobject-<version>.jar

If you have a custom settings.xml for maven already (for any reason) you can use your own. You might want to take a pick
at the one provided at the root of the project as it defines some additional repositories used to download some
dependencies and plugins the project needs to build correctly.