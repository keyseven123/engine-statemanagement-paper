# NebulaStream ![NES CI](https://github.com/nebulastream/nebulastream/workflows/NES%20CI/badge.svg)

NebulaStream is the first general purpose, end-to-end data management system for the IoT. It provides an out-of-the box experience with rich data processing functionalities and a high ease-of-use.

NebulaStream is a joint research project between the DIMA group at TU Berlin and the DFKI IAM group.

Learn more about Nebula Stream at https://www.nebula.stream/

Visit our wiki at http://iotdb.dfki.de/doku.php

## Documentation
- API:
    - [Query API](https://github.com/nebulastream/nebulastream/wiki/Query-API) 
    - [REST API](https://github.com/nebulastream/nebulastream/wiki/REST-API)     
- Development:
    - [How to contribute](https://github.com/nebulastream/nebulastream/wiki/How-to-contribute-to-NES)
    - [Building from Source](https://github.com/nebulastream/nebulastream/wiki/How-to-Build-and-Test) 
    - [Code Style](https://github.com/nebulastream/code-style) 
    - [Glossary](https://github.com/nebulastream/nebulastream/wiki/Glossary) 

  
## Components:

The codebase is structured in the following components:

| Component    | Description                                                                                                                 |
|--------------|-----------------------------------------------------------------------------------------------------------------------------|
| nes-common   | This component contains some base functionality that is used across all other components, e.g., for logging and exceptions. |
| nes-compiler | This component contains functionalities to compile source code or intermediate representations to executable binaries.      |
| nes-core     | This component contains the main aspects of the overall system.                                                             |
| nes-client   | This component contains the C++ client to interact with NebulaStream from C++ applications.                                 |
