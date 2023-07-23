Dockerfile for the set of operators provided
============================================

![repository view](/raw//googlebigquerydockerfile.png)

Steps to create your dockerfile:
* Open the Modeler, and select the `repository`tab.
* Expand the "dockerfiles", and press right mouse-button and select "Create Docker File"
* Give it a name: "googlebigquery" or a name of your choice (name not important, tags are)
* You will now have an empty Dockerfile - Open the DOckerfile by clicking and insert tecxt from this repo
* After inserting the Docker commands, click the  "Settings" icon <img src='/raw/dockerfilesettings.png' height='25'> and add the necessary tags
* Press "Save" and then "Build" from the same menu bar: <img src='/raw/dockerfilesettings.png' height='25'>

You will now have 2 files in the new docker folder; a Dockerfile and a Tags.json.

After Build completed (successfully), your new Custom operators have an environment with the necessary libriaries and tools in place.

