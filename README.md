storm-ecommerce
===============

An example of analytics for an ecommerce site

## INSTALLING
* Install nodejs
* Install the dependencies for nodejs
  * cd webapp
  * npm install

* Install the Storm's requirements
  * mvn clean install

## RUNNING
* Running the webapp:
 * cd webapp
 * node app.js

* Running the topology
 * mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.analytics.TopologyStarter

## Description

Example of analytics for an ecommerce site


