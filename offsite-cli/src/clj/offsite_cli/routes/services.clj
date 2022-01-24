(ns offsite-cli.routes.services
  (:require
    [reitit.swagger :as swagger]
    [reitit.swagger-ui :as swagger-ui]
    [reitit.ring.coercion :as coercion]
    [reitit.coercion.spec :as spec-coercion]
    [reitit.ring.middleware.muuntaja :as muuntaja]
    [reitit.ring.middleware.multipart :as multipart]
    [reitit.ring.middleware.parameters :as parameters]
    [offsite-cli.middleware.formats :as formats]
    [offsite-cli.init :as init]
    [offsite-cli.db.db-core :refer :all]
    [ring.util.http-response :refer :all]
    [clojure.java.io :as io]
    [xtdb.api :as xt]))

(defn service-routes []
  ["/api"
   {:coercion spec-coercion/coercion
    :muuntaja formats/instance
    :swagger {:id ::api}
    :middleware [;; query-params & form-params
                 parameters/parameters-middleware
                 ;; content-negotiation
                 muuntaja/format-negotiate-middleware
                 ;; encoding response body
                 muuntaja/format-response-middleware
                 ;; exception handling
                 coercion/coerce-exceptions-middleware
                 ;; decoding request body
                 muuntaja/format-request-middleware
                 ;; coercing response bodys
                 coercion/coerce-response-middleware
                 ;; coercing request parameters
                 coercion/coerce-request-middleware
                 ;; multipart
                 multipart/multipart-middleware]}

   ;; swagger documentation
   ["" {:no-doc true
        :swagger {:info {:title "Offsite Client API"
                         :description "https://cljdoc.org/d/metosin/reitit"}}}

    ["/swagger.json"
     {:get (swagger/create-swagger-handler)}]

    ["/api-docs/*"
     {:get (swagger-ui/create-swagger-ui-handler
             {:url "/api/swagger.json"
              :config {:validator-url nil}})}]]

   ["/ping"
    {:get (constantly (ok {:message "pong"}))}]

   ;; if left unprotected the test api is a DOS attack vector, additional safeguards need
   ;; to be implemented to make sure it isn't available in a production instance
   ["/test"
    {:swagger {:tags ["test"]}}

    ["/db-touch"
     {:put {:summary    "Writes a test document to the XTDB."
            ;:headers    {"Accept" "application/edn"}
            :parameters {:body {:doc string?}}
            :handler    (fn [{{{:keys [doc]} :body} :parameters}]
                          (let [inst (easy-ingest! [{:xt/id :test-api-touch-id
                                                     :doc   doc}])]
                            {:status 200
                             :body   {:tx-info inst}}))}}]

    ["/db-last-touch"
     {:get {:summary "Gets the last time the db-touch command was issued and full XTDB doc"
            :handler (fn [_]
                       (let [response (xt/entity-tx (xt/db db-node*) :test-api-touch-id)]
                         {:status 200
                          :body   {:tx-time (:xtdb.api/tx-time response)
                                   :entity  (get-entity! :test-api-touch-id)}}))}}]]


   ["/init"
    {:swagger {:tags ["init"]}}

    ["/get-backup-paths"
     {:get {:summary (str "display the current backup paths in: " (:paths-file @init/paths-config))
            :handler (fn [_]
                       (let [response (init/get-paths)]
                         {:status (if (not (nil? response)) 200 404)
                          :body   response}))}}]

    ["/reset-default-backup-paths"
     {:get {:summary "Reset to the default backup-paths.edn configuration file."
            :handler (fn [_]
                       {:status 200
                        :body   {:paths-file (init/reset-default-backup-paths)}})}}]

    ["/load-backup-paths"
     {:post {:summary    "Specify a custom location for a backup-paths.edn configuration file."
             :parameters {:body {:path string?}}
             :responses  {200 {:body {:path string?, :found boolean?}}}
             :handler    (fn [{{{:keys [path]} :body} :parameters}]
                           (let [backup-paths (init/get-paths path)]
                             {:status (if (not (nil? backup-paths)) 200 401)
                              :body   {:path path :found (not (nil? backup-paths))}}))}}]]

   ["/math"
    {:swagger {:tags ["math"]}}

    ["/plus"
     {:get {:summary "plus with spec query parameters"
            :parameters {:query {:x int?, :y int?}}
            :responses {200 {:body {:total pos-int?}}}
            :handler (fn [{{{:keys [x y]} :query} :parameters}]
                       {:status 200
                        :body {:total (+ x y)}})}
      :post {:summary "plus with spec body parameters"
             :parameters {:body {:x int?, :y int?}}
             :responses {200 {:body {:total pos-int?}}}
             :handler (fn [{{{:keys [x y]} :body} :parameters}]
                        {:status 200
                         :body {:total (+ x y)}})}}]]

   ["/files"
    {:swagger {:tags ["files"]}}

    ["/upload"
     {:post {:summary "upload a file"
             :parameters {:multipart {:file multipart/temp-file-part}}
             :responses {200 {:body {:name string?, :size int?}}}
             :handler (fn [{{{:keys [file]} :multipart} :parameters}]
                        {:status 200
                         :body {:name (:filename file)
                                :size (:size file)}})}}]

    ["/download"
     {:get {:summary "downloads a file"
            :swagger {:produces ["image/png"]}
            :handler (fn [_]
                       {:status 200
                        :headers {"Content-Type" "image/png"}
                        :body (-> "public/img/warning_clojure.png"
                                  (io/resource)
                                  (io/input-stream))})}}]]])
