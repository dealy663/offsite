# offsite-cli

generated using Luminus version "4.27"

Offsite Client - client node for members of the BUC (backup collective) group. BUC client pushes backups out to BUC nodes via the BUC service


## Prerequisites

You will need [Leiningen][1] 2.0 or above installed.

[1]: https://github.com/technomancy/leiningen

If on an Intel Mac do:
- brew install java8 or Java11 or Java13 or Java16 etc
- brew install leiningen
- brew install clojure
- brew install clojure/tools/clojure
- brew install gh
- brew install mongodb-community@5.0
- Install [NodeJS](https://nodejs.org/dist/v17.3.0/node-v17.3.0.pkg)

After all that, you should be able to run $> lein repl
After the repl starts you can run
user=>(start)       ;; this will start the web service that is accessible at http://localhost:3000


## Running

To start a web server for the application, run:

    lein run 

## License

Copyright © 2021 FIXME
