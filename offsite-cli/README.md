# offsite-cli

generated using Luminus version "4.27"

Offsite Client - client node for members of the BUC (backup collective) group. BUC client pushes backups out to BUC nodes via the BUC service


## Prerequisites

You will need [Leiningen][1] 2.0 or above installed.

[1]: https://github.com/technomancy/leiningen

If on an Intel Mac do:
- Java
  - Install java8 (going higher thank Java 8 is difficult for Apple M1 processors at this point)
    - I suggest you stay with [Zulu JDK8](https://cdn.azul.com/zulu/bin/zulu8.58.0.13-ca-jdk8.0.312-macosx_aarch64.dmg) for now if on Apple M1
  - On my Intel Mac Zulu JDK8 resulted in a strange libcrypto load error, so I wound up using AdoptOpen JDK 8.0.292.j9-adpt installed with [SDKMan](https://sdkman.io/install). This seems to be working just fine.  
- Install Clojure and build toolchain elements 
  - brew install leiningen
  - brew install clojure
  - brew install clojure/tools/clojure
  - brew install gh
- ~~I also had to download and modify the latest [XTDB](https://github.com/xtdb/xtdb) source from github. You'll find it as a submodule to offsite-cli in the ```xtdb``` directory.~~
- ~~I'm using the XTDB-LMDB combo, because I was able to find that the latest instance of LMDB has a build supporting M1, however XTDB doesn't reference it.~~
  - ~~To pull in the latest version I had to modify the subproject XTDB-LMDB which is in the module's directory. Update the lwjgl entries to reference version 3.3.0 and change the macos classifiers to: "natives-macos-arm64"~~
  - The latest versions of RocksDB now support Apple M1 and also offer multi-threaded write support.
    - Use sample-configs/dev-config-rocksdb.edn as a starting point to reference XTDB with the RocksDB backend.
  - Copy the appropriate .edn files from sample-configs to the rootdir offsite-cli.
    - backup-paths.edn
    - dev-config.edn (pick the right one to copy, currently rodksdb)
    - shadow-cljs.edn
    - test-config.edn
  - So far on Apple M1 I've had to manually install the XTDB jars by running ```$> lein install``` in the following projects: 
    - xtdb-core
    - xtdb-lmdb
  - On Linux X86_64 I was able to get the tests running with XTDB-LMDB using Java 17. I still had to manually install ```xtdb/modules/lmdb```.
    - $> lein install 
    - This put copy of ```xtdb-core-dev-SNAPSHOT.jar``` and ```xtdb-lmdb-dev-SNAPSHOT.JAR``` in ~/.m2/repository
  - I haven't had chance to try this on Apple X86_64 yet. But I expect we will probably have to create a new branch in the xtdb submodule and reference the  ```natives-macos``` classifier for intel in the lmdb ```project.clj``` file.
    - Even better we may be able to add some Clojure in the ```lmdb/project.clj``` file which chooses the right classifier based on the detected architecture. 
  - Once things settle down with XTDB on Apple M1 this step in the build will need to be revisited and fixed up.
- Intellij Idea
  - Import each module (offsite-cli, offsite-node, offsite-server) as a Leiningen external model. 
  - Be sure to set an appropriate JDK for the project (JDK 8, 11, 17 etc)
  - Create run configurations to launch the REPL (hopefully these can be shared via Git)
    - Your run configurations should launch with Leiningen
- brew install mongodb-community@5.0 (probably not needed for the offsite-cli, but will be for the service and maybe the node too)
- Install [NodeJS](https://nodejs.org/dist/v17.3.0/node-v17.3.0.pkg)
  - Install npm
  - Install react 
    - $> npm install react 
- Access the main page of offsite-cli http://localhost:3000
  - From there you should see a notification about compiling with shadow-cljs
  - Run shadow-cljs to compile the app
    - $> npx shadow-cljs watch app
  - Refresh the main page http://localhost:3000 and it should be ready
  - You can access the Swagger UI for the REST interface at http://localhost:3000/swagger-ui/index.html

After all that, you should be able to run $> lein repl
After the repl starts you can run
user=>(start)       ;; this will start the web service that is accessible at http://localhost:3000


## Running

To start a web server for the application, run:

    lein run 

## License

Copyright © 2021 FIXME
