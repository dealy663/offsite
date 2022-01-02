# offsite-cli

generated using Luminus version "4.27"

Offsite Client - client node for members of the BUC (backup collective) group. BUC client pushes backups out to BUC nodes via the BUC service


## Prerequisites

You will need [Leiningen][1] 2.0 or above installed.

[1]: https://github.com/technomancy/leiningen

If on an Intel Mac do:
- Install java8 (going higher thank Java 8 is difficult for Apple M1 processors at this point)
  - I suggest you stay with [Zulu JDK8](https://cdn.azul.com/zulu/bin/zulu8.58.0.13-ca-jdk8.0.312-macosx_aarch64.dmg) for now if on Apple M1
- Install Clojure and build toolchain elements 
  - brew install leiningen
  - brew install clojure
  - brew install clojure/tools/clojure
  - brew install gh
- I also had to download and modify the latest [XTDB](https://github.com/xtdb/xtdb) source from github
- I'm using the XTDB-LMDB combo, because I was able to find that the latest instance of LMDB has a build supporting M1, however XTDB doesn't reference it.
  - To pull in the latest version I had to modify the subproject XTDB-LMDB which is in the modules directory. Update the lwjgl entries to reference version 3.3.0 and change the macos classifiers to: "natives-macos-arm64"
  - So far I've had to manually install the XTDB jars by running ```$> lein install``` in the following projects: 
    - xtdb-core
    - xtdb-lmdb
    - http-server
    - jdbc
    - metrics
    - rocksdb
    - ** Note there is still a problem running ```$> lein test-refresh``` where it can't seem to locate fixtures.clj in the test module. However ```$> lein test``` works fine
- brew install mongodb-community@5.0 (probably not needed for the offsite-cli, but will be for the service and maybe the node too)
  - Install [NodeJS](https://nodejs.org/dist/v17.3.0/node-v17.3.0.pkg)

After all that, you should be able to run $> lein repl
After the repl starts you can run
user=>(start)       ;; this will start the web service that is accessible at http://localhost:3000


## Running

To start a web server for the application, run:

    lein run 

## License

Copyright © 2021 FIXME
