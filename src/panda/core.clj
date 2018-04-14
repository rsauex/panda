(ns panda.core
  (:require
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [aleph.udp :as udp]
    [aleph.tcp :as tcp]
    [clojure.string :as str]
    [byte-streams :as bs]
    [clojure.core.async :as a
     :refer [>! <! >!! <!! go chan buffer close! alts! alts!! timeout]])
  (:import
   [java.net SocketAddress InetSocketAddress InetAddress])
  (:gen-class))

;;; We have two queues:

;; for tasks to be sent
(def tasks-chan (chan 20))
;; for results received from clients
(def results-chan (chan 20))

;; UDP port for receiving connection requests
(def udp-port 48654)

(defn safe-println [& more]
  (.write *out* (str (clojure.string/join " " more) "\n"))
  (flush))

(defn add-client [address]
  ;; Connect to client
  (let [socket @(tcp/client {:remote-address address})]
    (d/loop []
      ;; Get next task
      (let [task (<!! tasks-chan)]
        (-> task
            (d/chain
             ;; Send task to client
             (fn [task]
               (safe-println "Got task: " task)
               (s/put! socket task))
             ;; Receive result
             (fn [r]
               (if r
                 (s/take! socket ::none)
                 ::none))
             ;; If successful put result into results channel
             ;; else put task back to tasks channel
             (fn [result]
               (safe-println "Response: " result)
               (if (= ::none result)
                 (>!! tasks-chan task)
                 (do (>!! results-chan result)
                     (d/recur)))))
            (d/catch
                (fn [ex]
                  (safe-println "Error: " ex)
                  (s/close! socket))))))))

(defn parse-connection-packet
  "Returns an address for tcp connection to client for
a given connection request message from client."
  [msg]
  (let [host  (.getAddress ^InetSocketAddress (:sender msg))
        value (bs/to-string (:message msg))
        port  (Long/parseLong value)]
    (InetSocketAddress. ^InetAddress host port)))

(defn log-new-connection [address]
  (safe-println (str "New connection: " address))
  address)

(defn start-connection-server
  "Creates a connection server which accepts connection requests
from clients and starts connection with client."
  []
  (safe-println "Start connection server")
  (let [server-socket @(udp/socket {:port udp-port})]
    (->> server-socket
         (s/map parse-connection-packet)
         (s/map log-new-connection)
         (s/consume add-client))))

;; Test client
(defn start-clnt []
  (let [server (tcp/start-server (fn [s i]
                                   (s/connect (s/map (fn [x] (safe-println "Got value: " x) x) s) s))
                                 {:port 0})
        port (aleph.netty/port server)]
    (safe-println "Clnt started: " port)
    (let [s-socket @(udp/socket {:broadcast? true})]
      (s/put! s-socket
              {:host "255.255.255.255"
               :port udp-port
               :message (str port)}))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Start")
  (start-connection-server)
  (go (Thread/sleep 1000)
      (start-clnt)
      (Thread/sleep 1000)
      (>!! tasks-chan "a")
      (Thread/sleep 1000)
      (>!! tasks-chan "b")
      (Thread/sleep 1000))
  (loop []
    (safe-println "-- Result: " (<!! results-chan))
    (recur)))
