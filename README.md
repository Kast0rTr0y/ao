# Active Objects

## Description

Active Objects is an ORM (object relational mapping) layer in Atlassian products, provided via the
[Active Objects Plugin](https://bitbucket.org/activeobjects/ao-plugin/).

The active objects project uses maven for its build. To build:

1. Install maven (3.3.3 or above):
    - you can find all about maven at [maven.apache.org](http://maven.apache.org/)
    - install instructions are [here](http://maven.apache.org/download.html#Installation)

2. Use the libraries from the ```repository``` directory by either:
    - Adding this directory as a repository to your maven configuration, or
    - Copying the content of that repository to your local maven repository: ```~/.m2/repository```

3. From the root of the project, run: ```mvn install```

4. That's it! You will find the active objects library under ```${basedir}/activeobjects/target/activeobject-<version>.jar```

## Atlassian Developer?

### Internal Documentation

[Development and maintenance documentation](https://ecosystem.atlassian.net/wiki/display/AO/Home)

### Committing Guidelines

Please see [The Platform Rules of Engagement (go/proe)](http://go.atlassian.com/proe) for committing to this module.

### Builds

The Bamboo builds for this project are on [EcoBAC](https://ecosystem.atlassian.net/wiki/display/AO/Home)

## External User?

### Issues

Please raise any issues you find with this module in [JIRA](https://ecosystem.atlassian.net/browse/AO)

### Documentation

[Active Objects Documentation](https://developer.atlassian.com/display/DOCS/Active+Objects)